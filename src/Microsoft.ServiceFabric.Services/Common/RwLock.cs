// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.ServiceFabric.Services.Common
{
    using System;
    using System.Threading;

    internal sealed class RwLock
    {
        private readonly ReaderWriterLockSlim rwLock;

        public RwLock()
        {
            this.rwLock = new ReaderWriterLockSlim();
        }

        public DisposableWriteLock AcquireWriteLock()
        {
            return new DisposableWriteLock(this.rwLock);
        }

        public DisposableReadLock AcquireReadLock()
        {
            return new DisposableReadLock(this.rwLock);
        }

        public struct DisposableReadLock : IDisposable
        {
            private ReaderWriterLockSlim rwLock;

            public DisposableReadLock(ReaderWriterLockSlim rwLock)
            {
                rwLock.EnterReadLock();
                this.rwLock = rwLock;
            }

            public void Dispose()
            {
                this.rwLock?.ExitReadLock();
                this.rwLock = null;
            }
        }

        public struct DisposableWriteLock : IDisposable
        {
            private ReaderWriterLockSlim rwLock;

            public DisposableWriteLock(ReaderWriterLockSlim rwLock)
            {
                rwLock.EnterWriteLock();
                this.rwLock = rwLock;
            }

            public void Dispose()
            {
                this.rwLock?.ExitWriteLock();
                this.rwLock = null;
            }
        }
    }
}
