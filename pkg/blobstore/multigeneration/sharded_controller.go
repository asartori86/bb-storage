package multigeneration

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/buildbarn/bb-storage/pkg/grpc"
	pb "github.com/buildbarn/bb-storage/pkg/proto/configuration/blobstore"
	mg_proto "github.com/buildbarn/bb-storage/pkg/proto/multigeneration"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

var request = &emptypb.Empty{}

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
			x.check()
		}
	}()
	return &x
}

func (c *shardedMultiGenerationController) check() {

	// check if clients need to rotate with a timeout equal to the next inspection time
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(c.timeInterval)*time.Second)
	defer cancel()
	status, err := c.checkStatus(ctx)
	if err != nil {
		log.Printf("Could not check all clientes: %s.\nRetry later\n", err)
		return
	}

	if status == mg_proto.MultiGenStatus_OK {
		return
	}
	// we don't want a timeout for acquiring and releasing the locks
	// since it can cause deadlocks if at least one client cannot release
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()
	ok, err := c.acquireAllRotateLocks(ctx)
	if err != nil {
		log.Printf("Could not aquire all rotateLocks and some might be left locked: %s\n", err)
		// TODO: try to recover by forcing a reset of the clients
		return
	}
	if !ok {
		log.Printf("Could not acquire all the rotateLocks, retry later.\n")
		return
	}

	if status == mg_proto.MultiGenStatus_ROTATION_NEEDED {
		err = c.callRotate(ctx)
		if err == nil {
			//all good
			return
		} else {
			log.Printf("Could not perform complete rotation: %v. retrying later\n", err)
			return
		}
	}
	if status == mg_proto.MultiGenStatus_RESET_NEEDED {
		log.Printf("resetting")
		c.callReset(ctx)
	}

}

func (c *shardedMultiGenerationController) callReset(ctx context.Context) (mg_proto.MultiGenStatus_Value, error) {
	// log.Printf("call checkRotate\n")
	for _, client := range c.clients {
		reply, err := client.DoReset(ctx, request)
		if err != nil {
			return mg_proto.MultiGenStatus_INTERNAL_ERROR, err
		}
		if reply.Status != mg_proto.MultiGenStatus_OK {
			// only one positive response is sufficient
			return reply.Status, nil
		}
	}
	return mg_proto.MultiGenStatus_OK, nil
}

func (c *shardedMultiGenerationController) checkStatus(ctx context.Context) (mg_proto.MultiGenStatus_Value, error) {
	// log.Printf("call checkRotate\n")
	for _, client := range c.clients {
		reply, err := client.GetStatus(ctx, request)
		if err != nil {
			return mg_proto.MultiGenStatus_INTERNAL_ERROR, err
		}
		if reply.Status != mg_proto.MultiGenStatus_OK {
			// only one positive response is sufficient
			return reply.Status, nil
		}
	}
	// no client needs to rotate and all RPCs went fine
	return mg_proto.MultiGenStatus_OK, nil
}

func (c *shardedMultiGenerationController) releaseAcquiredLocks(ctx context.Context, acquiredLocks []bool) error {
	for i, acquired := range acquiredLocks {
		if acquired {
			_, err := c.clients[i].ReleaseRotateLock(ctx, request)
			if err != nil {
				return fmt.Errorf("cannot release rotateLock on client %v: %s. This will likely cause a deadlock. Consider to restart the client", c.clients[i], err)
			}
		}
	}
	return nil
}

func (c *shardedMultiGenerationController) acquireAllRotateLocks(ctx context.Context) (bool, error) {
	for i, client := range c.clients {
		response, err := client.AcquireRotateLock(ctx, request)
		if response.Status != mg_proto.MultiGenStatus_OK {
			if err != nil {
				log.Println(err)
			}
			for j := i - 1; j >= 0; j-- {
				c.clients[j].ReleaseRotateLock(ctx, request)
			}
			return false, nil
		}
	}
	return true, nil
}

func (c *shardedMultiGenerationController) callRotate(ctx context.Context) error {
	for _, client := range c.clients {
		_, err := client.DoRotate(ctx, request)
		if err != nil {
			log.Printf("err")
			continue
		}
	}
	return nil
}
