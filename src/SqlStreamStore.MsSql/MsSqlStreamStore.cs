namespace SqlStreamStore
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Data.SqlClient;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.SqlServer.Server;
    using SqlStreamStore.Imports.Ensure.That;
    using SqlStreamStore.Infrastructure;
    using SqlStreamStore.MsSqlScripts;
    using SqlStreamStore.Subscriptions;

    internal class TransactionalDatabaseSessionWrapper
        : IDatabaseSession
    {
        readonly SqlConnection _connection;
        readonly bool _managedExternally;

        SqlTransaction _currentTransaction;
        bool _disposed;

        /// <summary>
        /// Constructs the wrapper, wrapping the given connection and transaction. It must be indicated with <paramref name="managedExternally"/> whether this wrapper
        /// should commit/rollback the transaction (depending on whether <see cref="Complete"/> is called before <see cref="Dispose()"/>), or if the transaction
        /// is handled outside of the wrapper
        /// </summary>
        public TransactionalDatabaseSessionWrapper(SqlConnection connection, SqlTransaction currentTransaction, bool managedExternally)
        {
            _connection = connection;
            _currentTransaction = currentTransaction;
            _managedExternally = managedExternally;

        }

        /// <summary>
        /// Creates a ready to used <see cref="SqlCommand"/>
        /// </summary>
        public SqlCommand CreateCommand(string commandText)
        {
            var sqlCommand = _connection.CreateCommand();
            sqlCommand.CommandText = commandText;
            sqlCommand.Transaction = _currentTransaction;
            return sqlCommand;
        }

        public SqlConnection Connection => _connection;
        public SqlTransaction Transaction => _currentTransaction;


        /// <summary>
        /// Marks that all work has been successfully done and the <see cref="SqlConnection"/> may have its transaction committed or whatever is natural to do at this time
        /// </summary>
        public void Complete()
        {
            if (_managedExternally) return;

            if (_currentTransaction == null)
                return;

            using (_currentTransaction)
            {
                _currentTransaction.Commit();
                _currentTransaction = null;
            }
        }

        /// <summary>
        /// Finishes the transaction and disposes the connection in order to return it to the connection pool. If the transaction
        /// has not been committed (by calling <see cref="Complete"/>), the transaction will be rolled back.
        /// </summary>
        public void Dispose()
        {
            if (_managedExternally) return;
            if (_disposed) return;

            try
            {
                try
                {
                    if (_currentTransaction != null)
                    {
                        using (_currentTransaction)
                        {
                            try
                            {
                                _currentTransaction.Rollback();
                            }
                            catch { }
                            _currentTransaction = null;
                        }
                    }
                }
                finally
                {
                    _connection.Dispose();
                }
            }
            finally
            {
                _disposed = true;
            }
        }
    }

    public class ExternalyManagedDatabaseSessionFactory
        : IDatabaseSessionFactory
    {
        private readonly Func<Tuple<SqlConnection, SqlTransaction>> _connection;

        public ExternalyManagedDatabaseSessionFactory(Func<Tuple<SqlConnection,SqlTransaction>> connection)
        {
            _connection = connection;
        }

        public async Task<IDatabaseSession> Create(CancellationToken cancellationToken)
        {
            var con = _connection();
            return new TransactionalDatabaseSessionWrapper(con.Item1,con.Item2, true);
        }
    }


    public class TransactionalDatabaseSessionFactory
        : IDatabaseSessionFactory
    {
        private readonly string _connectionString;
        private readonly IsolationLevel _isolationLevel;
        private readonly bool _externallyManagedTransactions;

        public TransactionalDatabaseSessionFactory(string connectionString, IsolationLevel isolationLevel = IsolationLevel.ReadCommitted, bool externallyManagedTransactions = false)
        {
            if (string.IsNullOrEmpty(connectionString))
            {
                throw new ArgumentException("message", nameof(connectionString));
            }

            _connectionString = connectionString;
            _isolationLevel = isolationLevel;
            _externallyManagedTransactions = externallyManagedTransactions;
        }

        public async Task<IDatabaseSession> Create(CancellationToken cancellationToken)
        {
            SqlConnection connection = null;

            try
            {

                connection = new SqlConnection(_connectionString);
                connection.Open();

                var transaction = connection.BeginTransaction(_isolationLevel);

                var conn = new TransactionalDatabaseSessionWrapper(connection, transaction, _externallyManagedTransactions);
                return await Task.FromResult(conn);
            }
            catch (Exception)
            {
                connection?.Dispose();
                throw;
            }
        }
    }

    public interface IDatabaseSession
        : IDisposable
    {

        SqlConnection Connection { get; }
        SqlTransaction Transaction { get; }

        void Complete();

        SqlCommand CreateCommand(string commandText);
    }

    public interface IDatabaseSessionFactory
    {
        Task<IDatabaseSession> Create(CancellationToken cancellationToken);
    }


    /// <summary>
    ///     Represents a Micrsoft SQL Server stream store implementation.
    /// </summary>
    public sealed partial class MsSqlStreamStore : StreamStoreBase
    {
        private readonly MsSqlStreamStoreSettings _settings;

        // private readonly Func<SqlConnection> _createConnection;
        private readonly IDatabaseSessionFactory _sessionFactory;
        private readonly Lazy<IStreamStoreNotifier> _streamStoreNotifier;
        private readonly Scripts _scripts;
        private readonly SqlMetaData[] _appendToStreamSqlMetadata;
        public const int FirstSchemaVersion = 1;
        public const int CurrentSchemaVersion = 2;

        /// <summary>
        ///     Initializes a new instance of <see cref="MsSqlStreamStore"/>
        /// </summary>
        /// <param name="settings">A settings class to configur this instance.</param>
        public MsSqlStreamStore(MsSqlStreamStoreSettings settings)
            : base(settings.MetadataMaxAgeCacheExpire, settings.MetadataMaxAgeCacheMaxSize,
                 settings.GetUtcNow, settings.LogName)
        {
            _settings = settings;
            Ensure.That(settings, nameof(settings)).IsNotNull();

            _sessionFactory = settings.Factory;


            _streamStoreNotifier = new Lazy<IStreamStoreNotifier>(() =>
                {
                    if (settings.CreateStreamStoreNotifier == null)
                    {
                        throw new InvalidOperationException(
                            "Cannot create notifier because supplied createStreamStoreNotifier was null");
                    }
                    return settings.CreateStreamStoreNotifier.Invoke(this);
                });
            _scripts = new Scripts(settings.Schema);

            var sqlMetaData = new List<SqlMetaData>
            {
                new SqlMetaData("StreamVersion", SqlDbType.Int, true, false, SortOrder.Unspecified, -1),
                new SqlMetaData("Id", SqlDbType.UniqueIdentifier),
                new SqlMetaData("Created", SqlDbType.DateTime, true, false, SortOrder.Unspecified, -1),
                new SqlMetaData("Type", SqlDbType.NVarChar, 128),
                new SqlMetaData("JsonData", SqlDbType.NVarChar, SqlMetaData.Max),
                new SqlMetaData("JsonMetadata", SqlDbType.NVarChar, SqlMetaData.Max)
            };

            if (settings.GetUtcNow != null)
            {
                // Created column value will be client supplied so should prevent using of the column default function
                sqlMetaData[2] = new SqlMetaData("Created", SqlDbType.DateTime);
            }

            _appendToStreamSqlMetadata = sqlMetaData.ToArray();
        }

        /// <summary>
        ///     Creates a scheme to hold stream 
        /// </summary>
        /// <param name="cancellationToken">The cancellation instruction.</param>
        /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
        public async Task CreateSchema(CancellationToken cancellationToken = default(CancellationToken))
        {
            GuardAgainstDisposed();

            using (var conn = new SqlConnection(_settings.ConnectionString))
            {
                await conn.OpenAsync(cancellationToken).NotOnCapturedContext();

                if (_scripts.Schema != "dbo")
                {
                    using (var command = new SqlCommand($@"
                        IF NOT EXISTS (
                        SELECT  schema_name
                        FROM    information_schema.schemata
                        WHERE   schema_name = '{_scripts.Schema}' ) 

                        BEGIN
                        EXEC sp_executesql N'CREATE SCHEMA {_scripts.Schema}'
                        END", conn))
                    {
                        await command
                            .ExecuteNonQueryAsync(cancellationToken)
                            .NotOnCapturedContext();
                    }
                }

                using (var command = new SqlCommand(_scripts.CreateSchema, conn))
                {
                    await command.ExecuteNonQueryAsync(cancellationToken)
                        .NotOnCapturedContext();
                }

            }
        }

        internal async Task CreateSchema_v1_ForTests(CancellationToken cancellationToken = default(CancellationToken))
        {
            GuardAgainstDisposed();

            using (var conn = new SqlConnection(_settings.ConnectionString))
            {
                await conn.OpenAsync(cancellationToken).NotOnCapturedContext();

                if (_scripts.Schema != "dbo")
                {
                    using (var command = new SqlCommand($@"
                        IF NOT EXISTS (
                        SELECT  schema_name
                        FROM    information_schema.schemata
                        WHERE   schema_name = '{_scripts.Schema}' ) 

                        BEGIN
                        EXEC sp_executesql N'CREATE SCHEMA {_scripts.Schema}'
                        END", conn))
                    {
                        await command
                            .ExecuteNonQueryAsync(cancellationToken)
                            .NotOnCapturedContext();
                    }
                }

                using (var command = new SqlCommand(_scripts.CreateSchema_v1, conn))
                {
                    await command.ExecuteNonQueryAsync(cancellationToken)
                        .NotOnCapturedContext();
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="cancellationToken"></param>
        /// <returns>A <see cref="CheckSchemaResult"/> representing the result of the operation.</returns>
        public async Task<CheckSchemaResult> CheckSchema(
            CancellationToken cancellationToken = default(CancellationToken))
        {
            GuardAgainstDisposed();

            using (var session = await _sessionFactory.Create(cancellationToken).NotOnCapturedContext())
            {
                using (var command = session.CreateCommand(_scripts.GetSchemaVersion))
                {
                    var extendedProperties = await command
                        .ExecuteReaderAsync(cancellationToken)
                        .NotOnCapturedContext();

                    int? version = null;
                    while (await extendedProperties.ReadAsync(cancellationToken))
                    {
                        if (extendedProperties.GetString(0) != "version")
                            continue;
                        version = int.Parse(extendedProperties.GetString(1));
                        break;
                    }

                    return version == null
                        ? new CheckSchemaResult(FirstSchemaVersion, CurrentSchemaVersion)  // First schema (1) didn't have extended properties.
                        : new CheckSchemaResult(int.Parse(version.ToString()), CurrentSchemaVersion);
                }
            }
        }

        /// <summary>
        ///     Drops all tables related to this store instance.
        /// </summary>
        /// <param name="cancellationToken">The cancellation instruction.</param>
        /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
        public async Task DropAll(CancellationToken cancellationToken = default(CancellationToken))
        {
            GuardAgainstDisposed();

            using (var session = await _sessionFactory.Create(cancellationToken).NotOnCapturedContext())
            {
                using (var command = session.CreateCommand(_scripts.DropAll))
                {
                    await command
                        .ExecuteNonQueryAsync(cancellationToken)
                        .NotOnCapturedContext();
                    session.Complete();
                }
            }
        }

        /// <inheritdoc />
        protected override async Task<int> GetStreamMessageCount(
            string streamId,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            GuardAgainstDisposed();

            using (var session = await _sessionFactory.Create(cancellationToken).NotOnCapturedContext())
            {
                using (var command = session.CreateCommand(_scripts.GetStreamMessageCount))
                {
                    var streamIdInfo = new StreamIdInfo(streamId);
                    command.Parameters.AddWithValue("streamId", streamIdInfo.SqlStreamId.Id);

                    var result = await command
                        .ExecuteScalarAsync(cancellationToken)
                        .NotOnCapturedContext();

                    return (int)result;
                }
            }
        }

        public async Task<int> GetmessageCount(
            string streamId,
            DateTime createdBefore,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            GuardAgainstDisposed();

            using (var session = await _sessionFactory.Create(cancellationToken).NotOnCapturedContext())
            {
                using (var command = session.CreateCommand(_scripts.GetStreamMessageBeforeCreatedCount))
                {
                    var streamIdInfo = new StreamIdInfo(streamId);
                    command.Parameters.AddWithValue("streamId", streamIdInfo.SqlStreamId.Id);
                    command.Parameters.AddWithValue("created", createdBefore);

                    var result = await command
                        .ExecuteScalarAsync(cancellationToken)
                        .NotOnCapturedContext();

                    return (int)result;
                }
            }
        }

        protected override async Task<long> ReadHeadPositionInternal(CancellationToken cancellationToken)
        {
            GuardAgainstDisposed();

            using (var session = await _sessionFactory.Create(cancellationToken).NotOnCapturedContext())
            {


                using (var command = session.CreateCommand(_scripts.ReadHeadPosition))
                {
                    var result = await command
                        .ExecuteScalarAsync(cancellationToken)
                        .NotOnCapturedContext();

                    if (result == DBNull.Value)
                    {
                        return -1;
                    }
                    return (long)result;
                }
            }
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                if (_streamStoreNotifier.IsValueCreated)
                {
                    _streamStoreNotifier.Value.Dispose();
                }
            }
            base.Dispose(disposing);
        }

        private IObservable<Unit> GetStoreObservable => _streamStoreNotifier.Value;
    }
}