package multigeneration

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/buildbarn/bb-storage/pkg/grpc"
	pb "github.com/buildbarn/bb-storage/pkg/proto/configuration/blobstore"
	mg_proto "github.com/buildbarn/bb-storage/pkg/proto/multigeneration"
)

type shardedMultiGenerationController struct {
	servers []mg_proto.ShardedMultiGenerationControllerClient
}

func NewShardedMultiGenerationControllerFromConfiguration(conf *pb.BlobAccessConfiguration, grpcClientFactory grpc.ClientFactory) *shardedMultiGenerationController {
	switch backend := conf.Backend.(type) {
	case *pb.BlobAccessConfiguration_ShardedMultiGeneration:
		nShards := len(backend.ShardedMultiGeneration.Shards)
		backends := make([]mg_proto.ShardedMultiGenerationControllerClient, 0, nShards)
		for _, shard := range backend.ShardedMultiGeneration.Shards {
			client, _ := grpcClientFactory.NewClientFromConfiguration(shard.Backend.GetGrpc())

			backend := mg_proto.NewShardedMultiGenerationControllerClient(client)

			backends = append(backends, backend)
		}
		return NewShardedMultiGenerationController(backends)
	}
	return nil
}

func NewShardedMultiGenerationController(servers []mg_proto.ShardedMultiGenerationControllerClient) *shardedMultiGenerationController {
	x := shardedMultiGenerationController{
		servers: servers,
	}
	go func() {
		tick := time.NewTicker(time.Duration(5 * uint64(time.Second)))
		for {
			<-tick.C
			x.checkRotate()
		}
	}()
	return &x
}

func (c *shardedMultiGenerationController) checkRotate() {
	log.Printf("call checkRotate\n")
	wantsToRotate := false
	wg := sync.WaitGroup{}
	for _, server := range c.servers {
		reply, err := server.GetIfWantsToRotate(context.TODO(), &mg_proto.MultiGenRequest{})
		if err != nil {
			log.Printf("ERRR %v\n", err)
		}
		if reply.Response {
			wantsToRotate = true
			break
		}
	}
	if !wantsToRotate {
		return
	}
	// acquire all the locks
	gotAllTheLocks := false
	waitTimeInterval := 1 * time.Second
	for !gotAllTheLocks {
		gotLock := make([]bool, len(c.servers))
		for i, server := range c.servers {
			reply, _ := server.TryAcquireRotateLock(context.TODO(), &mg_proto.MultiGenRequest{})
			gotLock[i] = reply.Response
		}
		tmp := true
		for _, b := range gotLock {
			tmp = tmp && b
		}
		gotAllTheLocks = tmp
		if !gotAllTheLocks {
			// release the eventually acquired locks
			for i, b := range gotLock {
				if b {
					c.servers[i].ReleaseRotateLock(context.TODO(), &mg_proto.MultiGenRequest{})
				}
			}
			time.Sleep(waitTimeInterval)
			waitTimeInterval *= 2
		}
	}
	// call rotate on all the servers in parallel
	for _, server := range c.servers {
		wg.Add(1)
		go func(server mg_proto.ShardedMultiGenerationControllerClient) {
			defer wg.Done()
			server.DoRotate(context.TODO(), &mg_proto.MultiGenRequest{})
		}(server)
	}
	wg.Wait()

	// release the locks

	for _, server := range c.servers {
		server.ReleaseRotateLock(context.Background(), &mg_proto.MultiGenRequest{})
	}
}
