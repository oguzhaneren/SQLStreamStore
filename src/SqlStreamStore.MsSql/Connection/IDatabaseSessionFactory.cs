namespace SqlStreamStore.Connection
{
    using System.Threading;
    using System.Threading.Tasks;

    public interface IDatabaseSessionFactory
    {
        Task<IDatabaseSession> Create(CancellationToken cancellationToken);
    }
}