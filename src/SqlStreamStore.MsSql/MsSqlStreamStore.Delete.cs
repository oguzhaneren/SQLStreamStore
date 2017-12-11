namespace SqlStreamStore
{
    using System;
    using System.Data.SqlClient;
    using System.Threading;
    using System.Threading.Tasks;
    using SqlStreamStore.Streams;
    using SqlStreamStore.Infrastructure;
    using static Streams.Deleted;

    public partial class MsSqlStreamStore
    {
        protected override Task DeleteStreamInternal(
            string streamId,
            int expectedVersion,
            CancellationToken cancellationToken)
        {
            var streamIdInfo = new StreamIdInfo(streamId);

            return expectedVersion == ExpectedVersion.Any
                ? DeleteStreamAnyVersion(streamIdInfo, cancellationToken)
                : DeleteStreamExpectedVersion(streamIdInfo, expectedVersion, cancellationToken);
        }

        protected override async Task DeleteEventInternal(
            string streamId,
            Guid eventId,
            CancellationToken cancellationToken)
        {
            using (var session = await _sessionFactory.Create(cancellationToken).NotOnCapturedContext())
            {
                var sqlStreamId = new StreamIdInfo(streamId).SqlStreamId;
                bool deleted;
                using (var command = session.CreateCommand(_scripts.DeleteStreamMessage))
                {
                    command.Parameters.AddWithValue("streamId", sqlStreamId.Id);
                    command.Parameters.AddWithValue("eventId", eventId);
                    var count = await command
                        .ExecuteScalarAsync(cancellationToken)
                        .NotOnCapturedContext();

                    deleted = (int)count == 1;
                }

                if (deleted)
                {
                    var eventDeletedEvent = CreateMessageDeletedMessage(sqlStreamId.IdOriginal, eventId);
                    await AppendToStreamExpectedVersionAny(
                        session,
                        SqlStreamId.Deleted,
                        new[] { eventDeletedEvent },
                        cancellationToken);
                }
                session.Complete();
            }
        }

        private async Task DeleteStreamExpectedVersion(
            StreamIdInfo streamIdInfo,
            int expectedVersion,
            CancellationToken cancellationToken)
        {
            using (var session = await _sessionFactory.Create(cancellationToken).NotOnCapturedContext())
            {
                using (var command = session.CreateCommand(_scripts.DeleteStreamExpectedVersion))
                {
                    command.Parameters.AddWithValue("streamId", streamIdInfo.SqlStreamId.Id);
                    command.Parameters.AddWithValue("expectedStreamVersion", expectedVersion);
                    try
                    {
                        await command
                            .ExecuteNonQueryAsync(cancellationToken)
                            .NotOnCapturedContext();
                    }
                    catch (SqlException ex)
                    {
                        // todo : check this
                        // transaction.Rollback();
                        if (ex.Message.StartsWith("WrongExpectedVersion"))
                        {
                            throw new WrongExpectedVersionException(
                                ErrorMessages.DeleteStreamFailedWrongExpectedVersion(
                                    streamIdInfo.SqlStreamId.IdOriginal, expectedVersion), ex);
                        }
                        throw;
                    }

                    var streamDeletedEvent = CreateStreamDeletedMessage(streamIdInfo.SqlStreamId.IdOriginal);
                    await AppendToStreamExpectedVersionAny(
                        session,
                        SqlStreamId.Deleted,
                        new[] { streamDeletedEvent },
                        cancellationToken);

                    // Delete metadata stream (if it exists)
                    await DeleteStreamAnyVersion(session, streamIdInfo.MetadataSqlStreamId, cancellationToken);

                    session.Complete();
                }
            }
        }

        private async Task DeleteStreamAnyVersion(
            StreamIdInfo streamIdInfo,
            CancellationToken cancellationToken)
        {
            using (var session = await _sessionFactory.Create(cancellationToken).NotOnCapturedContext())
            {

                await DeleteStreamAnyVersion(session, streamIdInfo.SqlStreamId, cancellationToken);

                // Delete metadata stream (if it exists)
                await DeleteStreamAnyVersion(session, streamIdInfo.MetadataSqlStreamId, cancellationToken);

                session.Complete();
            }
        }

        private async Task DeleteStreamAnyVersion(
           IDatabaseSession session,
           SqlStreamId sqlStreamId,
           CancellationToken cancellationToken)
        {
            bool aStreamIsDeleted;
            using (var command = session.CreateCommand(_scripts.DeleteStreamAnyVersion))
            {
                command.Parameters.AddWithValue("streamId", sqlStreamId.Id);
                var i = await command
                    .ExecuteScalarAsync(cancellationToken)
                    .NotOnCapturedContext();

                aStreamIsDeleted = (int)i > 0;
            }

            if (aStreamIsDeleted)
            {
                var streamDeletedEvent = CreateStreamDeletedMessage(sqlStreamId.IdOriginal);
                await AppendToStreamExpectedVersionAny(
                    session,
                    SqlStreamId.Deleted,
                    new[] { streamDeletedEvent },
                    cancellationToken);
            }
        }
    }
}
