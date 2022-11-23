package multigeneration

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/buildbarn/bb-storage/pkg/grpc"
	pb "github.com/buildbarn/bb-storage/pkg/proto/configuration/blobstore"
	mg_proto "github.com/buildbarn/bb-storage/pkg/proto/multigeneration"
)

var request = &mg_proto.MultiGenRequest{}

type shardedMultiGenerationController struct {
	clients      []mg_proto.ShardedMultiGenerationControllerClient
	timeInterval uint64
}

func NewShardedMultiGenerationControllerFromConfiguration(conf *pb.BlobAccessConfiguration, grpcClientFactory grpc.ClientFactory) (*shardedMultiGenerationController, error) {
	switch backend := conf.Backend.(type) {
	case *pb.BlobAccessConfiguration_ShardedMultiGeneration:
		nShards := len(backend.ShardedMultiGeneration.Shards)
		backends := make([]mg_proto.ShardedMultiGenerationControllerClient, 0, nShards)
		for _, shard := range backend.ShardedMultiGeneration.Shards {
			client, err := grpcClientFactory.NewClientFromConfiguration(shard.Backend.GetGrpc())
			if err != nil {
				return nil, fmt.Errorf("unable to setup grpc: %s", err)
			}

			backend := mg_proto.NewShardedMultiGenerationControllerClient(client)

			backends = append(backends, backend)
		}
		return NewShardedMultiGenerationController(backends, backend.ShardedMultiGeneration.QueryIntervalSeconds), nil
	}
	return nil, nil
}

func NewShardedMultiGenerationController(clients []mg_proto.ShardedMultiGenerationControllerClient, timeInterval uint64) *shardedMultiGenerationController {
	x := shardedMultiGenerationController{
		clients:      clients,
		timeInterval: timeInterval,
	}
	go func() {
		tick := time.NewTicker(time.Duration(x.timeInterval * uint64(time.Second)))
		for {
			<-tick.C
			x.checkRotate()
		}
	}()
	return &x
}

func (c *shardedMultiGenerationController) checkRotate() {
	log.Printf("call checkRotate\n")
	timeOut := time.Duration(c.timeInterval * uint64(time.Second))
	wantsToRotate := false
	wg := sync.WaitGroup{}
	ctx, cancel := context.WithTimeout(context.Background(), timeOut)
	defer cancel()
	for _, client := range c.clients {
		reply, _ := client.GetIfWantsToRotate(ctx, request)
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
		gotLock := make([]bool, len(c.clients))
		for i, client := range c.clients {
			reply, _ := client.TryAcquireRotateLock(ctx, request)
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
					c.clients[i].ReleaseRotateLock(ctx, request)
				}
			}
			time.Sleep(waitTimeInterval)
			waitTimeInterval *= 2
		}
	}
	// call rotate on all the servers in parallel
	for _, client := range c.clients {
		wg.Add(1)
		go func(client mg_proto.ShardedMultiGenerationControllerClient) {
			defer wg.Done()
			client.DoRotate(ctx, request)
		}(client)
	}
	wg.Wait()

	// release the locks
	for _, client := range c.clients {
		client.ReleaseRotateLock(ctx, request)
	}
}
