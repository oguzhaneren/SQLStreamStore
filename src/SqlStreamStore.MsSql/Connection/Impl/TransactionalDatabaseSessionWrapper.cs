namespace SqlStreamStore.Connection.Impl
{
    using System.Data.SqlClient;

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
}