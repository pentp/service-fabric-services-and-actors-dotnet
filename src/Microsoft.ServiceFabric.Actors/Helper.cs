// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.ServiceFabric.Actors
{
    using System;
    using Microsoft.ServiceFabric.Actors.Remoting;

    internal class Helper
    {
        public static string GetCallContext()
        {
            if (ActorLogicalCallContext.TryGet(out var callContextValue))
            {
                return callContextValue + Guid.NewGuid().ToString();
            }
            else
            {
                return Guid.NewGuid().ToString();
            }
        }
    }
}
