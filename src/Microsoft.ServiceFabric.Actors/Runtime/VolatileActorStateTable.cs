// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.ServiceFabric.Actors.Runtime
{
    using System;
    using System.Collections.Generic;
    using System.Fabric;
    using System.Runtime.Serialization;
    using System.Threading.Tasks;
    using Microsoft.ServiceFabric.Services.Common;

    internal sealed class VolatileActorStateTable<TType, TKey, TValue>
    {
        private readonly Dictionary<TType, Dictionary<TKey, TableEntry>> committedEntriesTable;

        /// <summary>
        /// Operations are only committed in sequence number order. This is needed
        /// to perform builds correctly - i.e. without sequence number "holes" in
        /// the copy data. ReplicationContext tracks whether a replication operation is
        ///
        ///     1) quorum acked
        ///     2) completed
        ///
        /// A replication operation is only completed when it is quorum acked and there
        /// are no other operations with lower sequence numbers that are not yet
        /// quorum acked.
        /// </summary>
        private readonly Dictionary<long, ReplicationContext> pendingReplicationContexts;

        /// <summary>
        /// Lists of entries are in non-decreasing sequence number order and used to
        /// take a snapshot of the current state when performing builds. The sequence numbers
        /// will not be contiguous if there were deletes.
        /// </summary>
        private readonly LinkedList<ListEntry> committedEntriesList;

        private readonly LinkedList<ListEntry> uncommittedEntriesList;
        private readonly RwLock rwLock;

        #region Public API

        public VolatileActorStateTable()
        {
            this.committedEntriesTable = new Dictionary<TType, Dictionary<TKey, TableEntry>>();
            this.pendingReplicationContexts = new Dictionary<long, ReplicationContext>();
            this.committedEntriesList = new LinkedList<ListEntry>();
            this.uncommittedEntriesList = new LinkedList<ListEntry>();
            this.rwLock = new RwLock();
        }

        public long GetHighestKnownSequenceNumber()
        {
            using (this.rwLock.AcquireReadLock())
            {
                return (this.uncommittedEntriesList.Last ?? this.committedEntriesList.Last)?.Value.ActorStateDataWrapper.SequenceNumber ?? 0;
            }
        }

        public long GetHighestCommittedSequenceNumber()
        {
            using (this.rwLock.AcquireReadLock())
            {
                return this.committedEntriesList.Last?.Value.ActorStateDataWrapper.SequenceNumber ?? 0;
            }
        }

        public void PrepareUpdate(List<ActorStateDataWrapper> actorStateDataWrapperList, long sequenceNumber)
        {
            // Invalid LSN
            if (sequenceNumber == 0)
            {
                return;
            }

            foreach (var actorStateDataWrapper in actorStateDataWrapperList)
            {
                actorStateDataWrapper.UpdateSequenceNumber(sequenceNumber);
            }

            using (this.rwLock.AcquireWriteLock())
            {
                var replicationContext = new ReplicationContext();

                foreach (var actorStateDataWrapper in actorStateDataWrapperList)
                {
                    var entry = new ListEntry(actorStateDataWrapper, replicationContext);
                    this.uncommittedEntriesList.AddLast(entry);
                }

                this.pendingReplicationContexts.Add(sequenceNumber, replicationContext);
            }
        }

        public Task CommitUpdateAsync(long sequenceNumber, Exception ex = null)
        {
            // Invalid LSN
            if (sequenceNumber == 0)
            {
                throw ex ?? new FabricException(FabricErrorCode.SequenceNumberCheckFailed);
            }

            // This list is used to store the replication contexts that have been commited
            // and are then marked as complete outside the read/write lock. Marking as complete
            // outside the lock is important because when the replication context is marked as
            // complete by calling TaskCompletionSource.SetResult(), the task associated with
            // TaskCompletionSource, immediately starts executing synchronously in the same thread
            // (while the lock still being held) which then tries to again acquire read/write lock
            // causing System.Threading.LockRecursionException.
            //
            // In .Net 4.6, TaskCompletionSource.SetResult() accepts an additional argument which
            // makes the task associated with TaskCompletionSource execute asynchronously on a different
            // thread. Till we move to .Net 4.6, we will adopt the above approach.
            var committedReplicationContexts = new List<ReplicationContext>();

            ReplicationContext replicationContext = null;

            using (this.rwLock.AcquireWriteLock())
            {
                replicationContext = this.pendingReplicationContexts[sequenceNumber];

                replicationContext.SetReplicationComplete(ex);

                if (sequenceNumber == this.uncommittedEntriesList.First.Value.ActorStateDataWrapper.SequenceNumber)
                {
                    while (this.uncommittedEntriesList.First is { Value: { IsReplicationComplete: true } } listNode)
                    {
                        this.uncommittedEntriesList.RemoveFirst();

                        if (!listNode.Value.IsFailed)
                        {
                            this.ApplyUpdate_UnderWriteLock(listNode);
                        }

                        listNode.Value.CompleteReplication();

                        var seqNum = listNode.Value.ActorStateDataWrapper.SequenceNumber;
                        var pendingContext = this.pendingReplicationContexts[seqNum];
                        if (pendingContext.IsAllEntriesComplete)
                        {
                            committedReplicationContexts.Add(pendingContext);
                            this.pendingReplicationContexts.Remove(seqNum);
                        }
                    }

                    replicationContext = null;
                }
            }

            // Mark the committed replication contexts as complete in order of increasing LSN
            foreach (var repCtx in committedReplicationContexts)
            {
                repCtx.MarkAsCompleted();
            }

            return replicationContext != null ? replicationContext.WaitForCompletionAsync() : TaskDone.Done;
        }

        public void ApplyUpdates(IEnumerable<ActorStateDataWrapper> actorStateDataList)
        {
            using (this.rwLock.AcquireWriteLock())
            {
                foreach (var actorStateData in actorStateDataList)
                {
                    this.ApplyUpdate_UnderWriteLock(new LinkedListNode<ListEntry>(
                        new ListEntry(actorStateData, null)));
                }
            }
        }

        public bool TryGetValue(TType type, TKey key, out TValue value)
        {
            using (this.rwLock.AcquireReadLock())
            {
                if (this.committedEntriesTable.TryGetValue(type, out var table) && table.TryGetValue(key, out var tableEntry))
                {
                    value = tableEntry.ActorStateDataWrapper.Value;
                    return true;
                }
            }

            value = default;
            return false;
        }

        public IEnumerator<TKey> GetSortedStorageKeyEnumerator(TType type, Func<TKey, bool> filter)
        {
            var committedStorageKeyList = new List<TKey>();

            using (this.rwLock.AcquireReadLock())
            {
                // Though VolatileActorStateTable can use SortedDictionary<>
                // which will also us to always get the keys in sorted order,
                // the lookup in SortedDictionary<> is O(logN) as opposed to
                // Dictionary<> which is average O(1).
                if (this.committedEntriesTable.TryGetValue(type, out var keyTable))
                {
                    foreach (var key in keyTable.Keys)
                    {
                        if (filter(key))
                        {
                            committedStorageKeyList.Add(key);
                        }
                    }
                }
            }

            // SortedList<> is designed to be used when we need the list
            // to be sorted through its intermediate stages. If the list
            // needs to be sorted only at the end of adding all entries
            // then using List<> is better.
            committedStorageKeyList.Sort();
            return committedStorageKeyList.GetEnumerator();
        }

        public List<TValue> GetActorStateValues(TType type)
        {
            var list = new List<TValue>();

            using (this.rwLock.AcquireReadLock())
            {
                if (this.committedEntriesTable.TryGetValue(type, out var keyTable))
                {
                    list.Capacity = keyTable.Count;
                    foreach (var entry in keyTable.Values)
                    {
                        list.Add(entry.ActorStateDataWrapper.Value);
                    }
                }
            }

            return list;
        }

        public List<TKey> GetActorStateKeys(TType type)
        {
            var list = new List<TKey>();

            using (this.rwLock.AcquireReadLock())
            {
                if (this.committedEntriesTable.TryGetValue(type, out var keyTable))
                {
                    list.Capacity = keyTable.Count;
                    foreach (var key in keyTable.Keys)
                    {
                        list.Add(key);
                    }
                }
            }

            return list;
        }

        public ActorStateEnumerator GetShallowCopiesEnumerator(TType type)
        {
            using (this.rwLock.AcquireReadLock())
            {
                var committedEntriesListShallowCopy = new List<ActorStateDataWrapper>();

                if (this.committedEntriesTable.TryGetValue(type, out var keyTable))
                {
                    foreach (var entry in keyTable.Values)
                    {
                        committedEntriesListShallowCopy.Add(entry.ActorStateDataWrapper);
                    }
                }

                return new ActorStateEnumerator(committedEntriesListShallowCopy, new List<ActorStateDataWrapper>());
            }
        }

        /// <summary>
        /// The use of read/write locks means that the process of creating shallow
        /// copies will necessarily compete with the replication operations. i.e.
        /// The process of preparing for a copy will block replication.
        /// </summary>
        public ActorStateEnumerator GetShallowCopiesEnumerator(long maxSequenceNumber)
        {
            var committedEntriesListShallowCopy = new List<ActorStateDataWrapper>();
            var uncommittedEntriesListShallowCopy = new List<ActorStateDataWrapper>();

            using (this.rwLock.AcquireReadLock())
            {
                long copiedSequenceNumber = 0;
                foreach (var entry in this.committedEntriesList)
                {
                    var wrapper = entry.ActorStateDataWrapper;
                    if (wrapper.SequenceNumber <= maxSequenceNumber)
                    {
                        copiedSequenceNumber = wrapper.SequenceNumber;

                        committedEntriesListShallowCopy.Add(wrapper);
                    }
                    else
                    {
                        break;
                    }
                }

                if (copiedSequenceNumber < maxSequenceNumber)
                {
                    foreach (var entry in this.uncommittedEntriesList)
                    {
                        var wrapper = entry.ActorStateDataWrapper;
                        if (wrapper.SequenceNumber <= maxSequenceNumber)
                        {
                            uncommittedEntriesListShallowCopy.Add(wrapper);
                        }
                        else
                        {
                            break;
                        }
                    }
                }
            }

            return new ActorStateEnumerator(
                committedEntriesListShallowCopy,
                uncommittedEntriesListShallowCopy);
        }

        #endregion Public API

        #region Helper methods

        private void ApplyUpdate_UnderWriteLock(LinkedListNode<ListEntry> listNode)
        {
            var stateData = listNode.Value.ActorStateDataWrapper;
            var type = stateData.Type;
            var key = stateData.Key;
            var isDelete = stateData.IsDelete;
            var newTableEntry = new TableEntry(stateData, listNode);

            if (!this.committedEntriesTable.TryGetValue(type, out var keyTable))
            {
                if (isDelete)
                {
                    // Nothing else to do
                    return;
                }

                keyTable = new Dictionary<TKey, TableEntry>();

                this.committedEntriesTable[type] = keyTable;
            }
            else if (keyTable.TryGetValue(key, out var oldTableEntry))
            {
                this.committedEntriesList.Remove(oldTableEntry.ListNode);
            }

            if (isDelete)
            {
                keyTable.Remove(key);
            }
            else
            {
                keyTable[key] = newTableEntry;
            }

            // The last element in the committed entries list must reflect the
            // sequence number of the last commit, which may be a delete.
            // If the current last element represents a delete, then it can
            // be removed now since we're adding a new last element.
            if (this.committedEntriesList.Last?.Value.ActorStateDataWrapper.IsDelete == true)
            {
                this.committedEntriesList.RemoveLast();
            }

            this.committedEntriesList.AddLast(listNode);
        }

        #endregion Helper methods

        #region Public inner classes

        [DataContract]
        public sealed class ActorStateDataWrapper
        {
            private ActorStateDataWrapper(
                TType type,
                TKey key,
                TValue value)
            {
                this.Type = type;
                this.Key = key;
                this.Value = value;
                this.IsDelete = false;
                this.SequenceNumber = 0;
            }

            private ActorStateDataWrapper(
                TType type,
                TKey key)
            {
                this.Type = type;
                this.Key = key;
                this.Value = default(TValue);
                this.IsDelete = true;
                this.SequenceNumber = 0;
            }

            [DataMember]
            public TType Type { get; private set; }

            [DataMember]
            public TKey Key { get; private set; }

            [DataMember]
            public TValue Value { get; private set; }

            [DataMember]
            public bool IsDelete { get; private set; }

            [DataMember]
            public long SequenceNumber { get; private set; }

            public static ActorStateDataWrapper CreateForUpdate(
                TType type,
                TKey key,
                TValue value)
            {
                return new ActorStateDataWrapper(type, key, value);
            }

            public static ActorStateDataWrapper CreateForDelete(
                TType type,
                TKey key)
            {
                return new ActorStateDataWrapper(type, key);
            }

            internal void UpdateSequenceNumber(long sequenceNumber)
            {
                this.SequenceNumber = sequenceNumber;
            }
        }

        public sealed class ActorStateEnumerator
        {
            private readonly List<ActorStateDataWrapper> committedEntriesListShallowCopy;
            private readonly List<ActorStateDataWrapper> uncommittedEntriesListShallowCopy;

            private int index;

            public ActorStateEnumerator(
                List<ActorStateDataWrapper> committedEntriesList,
                List<ActorStateDataWrapper> uncommittedEntriesList)
            {
                this.committedEntriesListShallowCopy = committedEntriesList;
                this.uncommittedEntriesListShallowCopy = uncommittedEntriesList;
            }

            public int CommittedCount => this.committedEntriesListShallowCopy.Count;

            public int UncommittedCount => this.uncommittedEntriesListShallowCopy.Count;

            public ActorStateDataWrapper PeekNext()
            {
                var committedCount = this.committedEntriesListShallowCopy.Count;
                var uncommittedCount = this.uncommittedEntriesListShallowCopy.Count;

                var next = this.index;

                if (next < committedCount)
                {
                    return this.committedEntriesListShallowCopy[next];
                }
                else if (next < uncommittedCount + committedCount)
                {
                    return this.uncommittedEntriesListShallowCopy[next - committedCount];
                }
                else
                {
                    return null;
                }
            }

            public void MoveNext() => this.index++;
        }

        #endregion Public inner classes

        #region Private inner classes

        private sealed class ReplicationContext
        {
            private readonly TaskCompletionSource<object> pendingCommitTaskSource;
            private Exception replicationException;
            private long associatedEntryCount;

            public ReplicationContext()
            {
                this.IsReplicationComplete = false;
                this.replicationException = null;
                this.pendingCommitTaskSource = new TaskCompletionSource<object>();
                this.associatedEntryCount = 0;
            }

            public bool IsReplicationComplete { get; private set; }

            public bool IsFailed
            {
                get
                {
                    return (this.replicationException != null);
                }
            }

            public bool IsAllEntriesComplete
            {
                get { return (this.associatedEntryCount == 0); }
            }

            public void SetReplicationComplete(Exception replicationException)
            {
                this.IsReplicationComplete = true;
                this.replicationException = replicationException;
            }

            public void AssociateListEntry()
            {
                this.associatedEntryCount++;
            }

            public void CompleteListEntry()
            {
                this.associatedEntryCount--;
            }

            public void MarkAsCompleted()
            {
                if (this.replicationException != null)
                {
                    this.pendingCommitTaskSource.SetException(this.replicationException);
                }
                else
                {
                    this.pendingCommitTaskSource.SetResult(null);
                }
            }

            public Task WaitForCompletionAsync()
            {
                return this.pendingCommitTaskSource.Task;
            }
        }

        private sealed class ListEntry
        {
            public ListEntry(
                ActorStateDataWrapper actorStateDataWrapper,
                ReplicationContext replicationContext)
            {
                this.ActorStateDataWrapper = actorStateDataWrapper;
                this.PendingReplicationContext = replicationContext;
                replicationContext?.AssociateListEntry();
            }

            public bool IsReplicationComplete => this.PendingReplicationContext?.IsReplicationComplete ?? true;

            public bool IsFailed => this.PendingReplicationContext?.IsFailed ?? false;

            public ActorStateDataWrapper ActorStateDataWrapper { get; }

            private ReplicationContext PendingReplicationContext { get; set; }

            public void CompleteReplication()
            {
                this.PendingReplicationContext?.CompleteListEntry();
                this.PendingReplicationContext = null;
            }
        }

#pragma warning disable SA1201 // Elements should appear in the correct order
        private readonly struct TableEntry
        {
            public TableEntry(
                ActorStateDataWrapper actorStateDataWrapper,
                LinkedListNode<ListEntry> listNode)
            {
                this.ActorStateDataWrapper = actorStateDataWrapper;
                this.ListNode = listNode;
            }

            public readonly ActorStateDataWrapper ActorStateDataWrapper;
            public readonly LinkedListNode<ListEntry> ListNode;
        }

        #endregion Private inner classes
    }
}
