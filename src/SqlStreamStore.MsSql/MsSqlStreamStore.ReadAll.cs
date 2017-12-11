    namespace SqlStreamStore
{
    using System;
    using System.Collections.Generic;
    using System.Data.SqlClient;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using SqlStreamStore.Streams;
    using SqlStreamStore.Infrastructure;

    public partial class MsSqlStreamStore
    {
        protected override async Task<ReadAllPage> ReadAllForwardsInternal(
            long fromPositionExlusive,
            int maxCount,
            bool prefetch,
            ReadNextAllPage readNext,
            CancellationToken cancellationToken)
        {
            maxCount = maxCount == int.MaxValue ? maxCount - 1 : maxCount;
            long ordinal = fromPositionExlusive;

            using (var session = await _connectionFactory.Create(cancellationToken).NotOnCapturedContext())
            {
                var commandText = prefetch ? _scripts.ReadAllForwardWithData : _scripts.ReadAllForward;
                using (var command = session.CreateCommand(commandText))
                {
                    command.Parameters.AddWithValue("ordinal", ordinal);
                    command.Parameters.AddWithValue("count", maxCount + 1); //Read extra row to see if at end or not
                    var reader = await command
                        .ExecuteReaderAsync(cancellationToken)
                        .NotOnCapturedContext();

                    List<StreamMessage> messages = new List<StreamMessage>();
                    if (!reader.HasRows)
                    {
                        return new ReadAllPage(
                            fromPositionExlusive,
                            fromPositionExlusive,
                            true,
                            ReadDirection.Forward,
                            readNext,
                            messages.ToArray());
                    }

                    while (await reader.ReadAsync(cancellationToken).NotOnCapturedContext())
                    {
                        if(messages.Count == maxCount)
                        {
                            messages.Add(default(StreamMessage));
                        }
                        else
                        {
                            var streamId = reader.GetString(0);
                            var streamVersion = reader.GetInt32(1);
                            ordinal = reader.GetInt64(2);
                            var eventId = reader.GetGuid(3);
                            var created = reader.GetDateTime(4);
                            var type = reader.GetString(5);
                            var jsonMetadata = reader.GetString(6);

                            Func<CancellationToken, Task<string>> getJsonData;
                            if(prefetch)
                            {
                                var jsonData = reader.GetString(7);
                                getJsonData = _ => Task.FromResult(jsonData);
                            }
                            else
                            {
                                var streamIdInfo = new StreamIdInfo(streamId);
                                getJsonData = ct => GetJsonData(streamIdInfo.SqlStreamId.Id, streamVersion, ct);
                            }

                            var message = new StreamMessage(streamId,
                                eventId,
                                streamVersion,
                                ordinal,
                                created,
                                type,
                                jsonMetadata,
                                getJsonData);

                            messages.Add(message);
                        }
                    }

                    bool isEnd = true;

                    if (messages.Count == maxCount + 1) // An extra row was read, we're not at the end
                    {
                        isEnd = false;
                        messages.RemoveAt(maxCount);
                    }

                    var nextPosition = messages[messages.Count - 1].Position + 1;

                    return new ReadAllPage(
                        fromPositionExlusive,
                        nextPosition,
                        isEnd,
                        ReadDirection.Forward,
                        readNext,
                        messages.ToArray());
                }
            }
        }

        protected override async Task<ReadAllPage> ReadAllBackwardsInternal(
            long fromPositionExclusive,
            int maxCount,
            bool prefetch,
            ReadNextAllPage readNext,
            CancellationToken cancellationToken)
        {
            maxCount = maxCount == int.MaxValue ? maxCount - 1 : maxCount;
            long ordinal = fromPositionExclusive == Position.End ? long.MaxValue : fromPositionExclusive;

            using (var session = await _connectionFactory.Create(cancellationToken).NotOnCapturedContext())
            {
                var commandText = prefetch ? _scripts.ReadAllBackwardWithData : _scripts.ReadAllBackward;
                using (var command = session.CreateCommand(commandText))
                {
                    command.Parameters.AddWithValue("ordinal", ordinal);
                    command.Parameters.AddWithValue("count", maxCount + 1); //Read extra row to see if at end or not
                    var reader = await command
                        .ExecuteReaderAsync(cancellationToken)
                        .NotOnCapturedContext();

                    List<StreamMessage> messages = new List<StreamMessage>();
                    if (!reader.HasRows)
                    {
                        // When reading backwards and there are no more items, then next position is LongPosition.Start,
                        // regardles of what the fromPosition is.
                        return new ReadAllPage(
                            Position.Start,
                            Position.Start,
                            true,
                            ReadDirection.Backward,
                            readNext,
                            messages.ToArray());
                    }

                    long lastOrdinal = 0;
                    while (await reader.ReadAsync(cancellationToken).NotOnCapturedContext())
                    {
                        var streamId = reader.GetString(0);
                        var streamVersion = reader.GetInt32(1);
                        ordinal = reader.GetInt64(2);
                        var eventId = reader.GetGuid(3);
                        var created = reader.GetDateTime(4);
                        var type = reader.GetString(5);
                        var jsonMetadata = reader.GetString(6);

                        Func<CancellationToken, Task<string>> getJsonData;
                        if (prefetch)
                        {
                            var jsonData = reader.GetString(7);
                            getJsonData = _ => Task.FromResult(jsonData);
                        }
                        else
                        {
                            var streamIdInfo = new StreamIdInfo(streamId);
                            getJsonData = ct => GetJsonData(streamIdInfo.SqlStreamId.Id, streamVersion, ct);
                        }

                        var message = new StreamMessage(
                            streamId,
                            eventId,
                            streamVersion,
                            ordinal,
                            created,
                            type,
                            jsonMetadata, getJsonData);

                        messages.Add(message);
                        lastOrdinal = ordinal;
                    }

                    bool isEnd = true;
                    var nextPosition = lastOrdinal;

                    if (messages.Count == maxCount + 1) // An extra row was read, we're not at the end
                    {
                        isEnd = false;
                        messages.RemoveAt(maxCount);
                    }

                    fromPositionExclusive = messages.Any() ? messages[0].Position : 0;

                    return new ReadAllPage(
                        fromPositionExclusive,
                        nextPosition,
                        isEnd,
                        ReadDirection.Backward,
                        readNext,
                        messages.ToArray());
                }
            }
        }
    }
}