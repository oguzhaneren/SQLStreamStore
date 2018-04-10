namespace SqlStreamStore.Connection
{
    using System;
    using System.Data.SqlClient;

    public interface IDatabaseSession
        : IDisposable
    {
        SqlConnection Connection { get; }
        SqlTransaction Transaction { get; }
        void Complete();
        SqlCommand CreateCommand(string commandText);
    }
}