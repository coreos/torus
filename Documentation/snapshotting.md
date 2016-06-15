# Snapshotting Torus Block Volumes

Snapshots of Torus Block Volumes can be taken at any time, as of the last sync() of the volume, even while mounted. Snapshots are Copy on Write and don't require a full copy; only the storage used in the past and any updates to the storage will count toward the total usage.

## Create a snapshot

```
torusctl block snapshot create myVolume@mySnapshotName
```

Creates a snapshot of the current state of myVolume called mySnapshotName. To list all current snapshots, use

```
torusctl block snapshot list myVolume
```

Remember that this operation is 'free' and does not require locking the volume.

## Delete a snapshot

```
torusctl block snapshot delete myVolume@mySnapshotName
```

Data that's unused will then be freed.

## Restore a snapshot

This operation, because it changes the state of the volume, requires that myVolume be unmounted.

```
torusctl block snapshot restore myVolume@mySnapshotName
```
