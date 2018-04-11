namespace SqlStreamStore.Connection.Impl
{
    using System;
    using System.Data.SqlClient;
    using System.Threading;
    using System.Threading.Tasks;
  

    public class ExternalyManagedDatabaseSessionFactory
        : IDatabaseSessionFactory
    {
        private readonly Func<Task<Tuple<SqlConnection, SqlTransaction>>> _connectionFactory;

        public ExternalyManagedDatabaseSessionFactory(
            Func<Task<Tuple<SqlConnection,SqlTransaction>>> connectionFactory)
        {
            _connectionFactory = connectionFactory;
        }

        public async Task<IDatabaseSession> Create(CancellationToken cancellationToken)
        {
            var con = await _connectionFactory().ConfigureAwait(false);
            return new TransactionalDatabaseSessionWrapper(con.Item1,con.Item2, true);
        }
    }
}