// Package bootstrap wires command domains together at application startup.
// New domains are imported and registered here, never inside wsserver.Handle.
package bootstrap

import (
	"context"

	"HomeCloudBackend/internal/commands"
	"HomeCloudBackend/internal/commands/clients"
	eventcommands "HomeCloudBackend/internal/commands/events"
	storagecommands "HomeCloudBackend/internal/commands/storage"
	streamcommands "HomeCloudBackend/internal/commands/streams"
	"HomeCloudBackend/internal/commands/system"
	eventbus "HomeCloudBackend/internal/events"
	store "HomeCloudBackend/internal/storage"
	"HomeCloudBackend/internal/streaming"
)

// Dependencies are provided by the hosting application. The commands modules
// must not import wsserver or touch sockets directly.
type Dependencies struct {
	Online              func() int
	ConnectedDevices    func() []map[string]string
	QueueToDevice       func(context.Context, string, commands.Outbound) error
	RequireSubscription bool
	PublishEvent        func(context.Context, string, string, any) (eventbus.PublishResult, error)
	StreamCreate        func(string, streaming.CreateRequest, string) (streaming.Info, error)
	StreamCancel        func(string, string) error
	StreamList          func(string) []streaming.Info
	StreamPath          string
	StorageRegister     func(context.Context, string, string, int) (store.Volume, error)
	StorageList         func(context.Context) ([]store.Volume, error)
	StorageFiles        func(context.Context, string, int) ([]store.File, error)
	StorageScan         func(context.Context, string, bool, int) (store.ScanResult, error)
	StorageIndex        func(context.Context, store.IndexRequest, bool) (store.IndexJob, error)
	StorageIndexStatus  func(string) (store.IndexJob, error)
	StorageIndexCancel  func(string) error
	StorageGetFile      func(context.Context, string) (store.File, error)
	StorageUpload       func(context.Context, string, string, string, string, int64) (store.TransferInfo, error)
	StorageDownload     func(context.Context, string, string) (store.TransferInfo, error)
	StorageCancel       func(string, string) error
}

func Register(r *commands.Registry, d Dependencies) {
	system.Register(r, system.Dependencies{
		Online:              d.Online,
		RequireSubscription: d.RequireSubscription,
	})
	clients.Register(r, clients.Dependencies{Connected: d.ConnectedDevices, QueueToDevice: d.QueueToDevice})
	eventcommands.Register(r, eventcommands.Dependencies{Publish: d.PublishEvent})
	streamcommands.Register(r, streamcommands.Dependencies{Create: d.StreamCreate, Cancel: d.StreamCancel, List: d.StreamList, QueueToDevice: d.QueueToDevice, Path: d.StreamPath})
	storagecommands.Register(r, storagecommands.Dependencies{Register: d.StorageRegister, List: d.StorageList, ListFiles: d.StorageFiles, Scan: d.StorageScan, StartIndex: d.StorageIndex, IndexStatus: d.StorageIndexStatus, CancelIndex: d.StorageIndexCancel, GetFile: d.StorageGetFile, PrepareUpload: d.StorageUpload, PrepareDownload: d.StorageDownload, Cancel: d.StorageCancel})
}
