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

    public interface IDatabaseSession
        :IDisposable
    {
        SqlCommand Commands { get; }
        SqlConnection Connection { get; }
        SqlTransaction Transaction { get; }

        void Complete();

        SqlCommand CreateCommand(string commandText);
    }

    public interface IDatabaseConnectionFactory
    {
        Task<IDatabaseSession> Create(CancellationToken cancellationToken);
    }


    /// <summary>
    ///     Represents a Micrsoft SQL Server stream store implementation.
    /// </summary>
    public sealed partial class MsSqlStreamStore : StreamStoreBase
    {
       // private readonly Func<SqlConnection> _createConnection;
        private readonly IDatabaseConnectionFactory _connectionFactory;
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
            :base(settings.MetadataMaxAgeCacheExpire, settings.MetadataMaxAgeCacheMaxSize,
                 settings.GetUtcNow, settings.LogName)
        {
            Ensure.That(settings, nameof(settings)).IsNotNull();

          //  _createConnection = () => new SqlConnection(settings.ConnectionString);
            _streamStoreNotifier = new Lazy<IStreamStoreNotifier>(() =>
                {
                    if(settings.CreateStreamStoreNotifier == null)
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

            if(settings.GetUtcNow != null)
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

            using(var session = await _connectionFactory.Create(cancellationToken).NotOnCapturedContext())
            {
              

                if(_scripts.Schema != "dbo")
                {
                    using(var command = session.CreateCommand($@"
                        IF NOT EXISTS (
                        SELECT  schema_name
                        FROM    information_schema.schemata
                        WHERE   schema_name = '{_scripts.Schema}' ) 

                        BEGIN
                        EXEC sp_executesql N'CREATE SCHEMA {_scripts.Schema}'
                        END"))
                    {
                        await command
                            .ExecuteNonQueryAsync(cancellationToken)
                            .NotOnCapturedContext();
                    }
                }

                using (var command = session.CreateCommand(_scripts.CreateSchema))
                {
                    await command.ExecuteNonQueryAsync(cancellationToken)
                        .NotOnCapturedContext();
                }
            }
        }

        internal async Task CreateSchema_v1_ForTests(CancellationToken cancellationToken = default(CancellationToken))
        {
            GuardAgainstDisposed();

            using (var session = await _connectionFactory.Create(cancellationToken).NotOnCapturedContext())
            {
                

                if (_scripts.Schema != "dbo")
                {
                    using (var command =session.CreateCommand($@"
                        IF NOT EXISTS (
                        SELECT  schema_name
                        FROM    information_schema.schemata
                        WHERE   schema_name = '{_scripts.Schema}' ) 

                        BEGIN
                        EXEC sp_executesql N'CREATE SCHEMA {_scripts.Schema}'
                        END"))
                    {
                        await command
                            .ExecuteNonQueryAsync(cancellationToken)
                            .NotOnCapturedContext();
                    }
                }

                using (var command = session.CreateCommand(_scripts.CreateSchema_v1))
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

            using (var session = await _connectionFactory.Create(cancellationToken).NotOnCapturedContext())
            {
                using (var command = session.CreateCommand(_scripts.GetSchemaVersion))
                {
                    var extendedProperties =  await command
                        .ExecuteReaderAsync(cancellationToken)
                        .NotOnCapturedContext();

                    int? version = null;
                    while(await extendedProperties.ReadAsync(cancellationToken))
                    {
                        if(extendedProperties.GetString(0) != "version")
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

            using (var session = await _connectionFactory.Create(cancellationToken).NotOnCapturedContext())
            {
                using(var command = session.CreateCommand(_scripts.DropAll))
                {
                    await command
                        .ExecuteNonQueryAsync(cancellationToken)
                        .NotOnCapturedContext();
                }
            }
        }

        /// <inheritdoc />
        protected override async Task<int> GetStreamMessageCount(
            string streamId,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            GuardAgainstDisposed();

            using (var session = await _connectionFactory.Create(cancellationToken).NotOnCapturedContext())
            {
                using(var command = session.CreateCommand(_scripts.GetStreamMessageCount))
                {
                    var streamIdInfo = new StreamIdInfo(streamId);
                    command.Parameters.AddWithValue("streamId", streamIdInfo.SqlStreamId.Id);

                    var result = await command
                        .ExecuteScalarAsync(cancellationToken)
                        .NotOnCapturedContext();

                    return (int) result;
                }
            }
        }

        public async Task<int> GetmessageCount(
            string streamId,
            DateTime createdBefore,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            GuardAgainstDisposed();

            using(var session = await _connectionFactory.Create(cancellationToken).NotOnCapturedContext())
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

            using (var session = await _connectionFactory.Create(cancellationToken).NotOnCapturedContext())
            {
               

                using(var command = session.CreateCommand(_scripts.ReadHeadPosition))
                {
                    var result = await command
                        .ExecuteScalarAsync(cancellationToken)
                        .NotOnCapturedContext();

                    if(result == DBNull.Value)
                    {
                        return -1;
                    }
                    return (long) result;
                }
            }
        }

        protected override void Dispose(bool disposing)
        {
            if(disposing)
            {
                if(_streamStoreNotifier.IsValueCreated)
                {
                    _streamStoreNotifier.Value.Dispose();
                }
            }
            base.Dispose(disposing);
        }

        private IObservable<Unit> GetStoreObservable => _streamStoreNotifier.Value;
    }
}