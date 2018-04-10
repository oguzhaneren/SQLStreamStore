namespace SqlStreamStore.Connection.Impl
{
    using System;
    using System.Data;
    using System.Data.SqlClient;
    using System.Threading;
    using System.Threading.Tasks;

    internal class TransactionalDatabaseSessionFactory
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
                await connection.OpenAsync(cancellationToken).ConfigureAwait(false);

                var transaction = connection.BeginTransaction(_isolationLevel);

                var conn = new TransactionalDatabaseSessionWrapper(connection, transaction, _externallyManagedTransactions);
                return conn;
            }
            catch (Exception)
            {
                connection?.Dispose();
                throw;
            }
        }
    }
}