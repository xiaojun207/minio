## Decommission pool

### How to decommission a pool?

Lists all currently configured pools.
```
~ mc admin pool list alias/ (lists all pools)
```

Drains all the objects from a POOL and spreads them across remaining pools.
```
~ mc admin pool drain alias/ POOL (drains and spreads across remaining pools)
```

Suspend writes on a pool.
```
~ mc admin pool suspend alias/ POOL
```

Resumes a previously suspended pool.
```
~ mc admin pool resume alias/ POOL
```

Provides current draining info.
```
~ mc admin pool info alias/ POOL
```
