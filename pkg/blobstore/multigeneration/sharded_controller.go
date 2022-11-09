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
			x.coordinateRotations()
		}
	}()
	return &x
}

func (c *shardedMultiGenerationController) coordinateRotations() {

	// check if clients need to rotate with a timeout equal to the next inspection time
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(c.timeInterval)*time.Second)
	defer cancel()
	mustRotate, err := c.checkRotate(ctx)
	if err != nil {
		log.Printf("Could not check all clientes: %s.\nRetry later\n", err)
		return
	}
	if !mustRotate {
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
	err = c.callRotate(ctx)
	if err != nil {
		log.Printf("Could not perform complete rotation: %v\n", err)
		// TODO: try to recover by forcing a reset of the clients
		return
	}
}

func (c *shardedMultiGenerationController) checkRotate(ctx context.Context) (bool, error) {
	log.Printf("call checkRotate\n")
	for _, client := range c.clients {
		reply, err := client.GetIfWantsToRotate(ctx, request)
		if err != nil {
			return false, err
		}
		if reply.Response {
			// only one positive response is sufficient
			return true, nil
		}
	}
	// no client needs to rotate and all RPCs went fine
	return false, nil
}

func (c *shardedMultiGenerationController) releaseAcquiredLocks(ctx context.Context, acquiredLocks []bool) error {
	for i, acquired := range acquiredLocks {
		if acquired {
			_, err := c.clients[i].ReleaseRotateLock(context.TODO(), request)
			if err != nil {
				return fmt.Errorf("cannot release rotateLock on client %v: %s. This will likely cause a deadlock. Consider to restart the client", c.clients[i], err)
			}
		}
	}
	return nil
}

func (c *shardedMultiGenerationController) acquireAllRotateLocks(ctx context.Context) (bool, error) {

	acquiredLocks := make([]bool, len(c.clients))
	wg := sync.WaitGroup{}
	for i, client := range c.clients {
		wg.Add(1)
		go func(i int, client mg_proto.ShardedMultiGenerationControllerClient) {
			defer wg.Done()
			_, err := client.AcquireRotateLock(ctx, request)
			if err != nil {
				log.Println(err)
			} else {
				acquiredLocks[i] = true
			}
		}(i, client)
	}
	wg.Wait()
	// check if we got all the locks
	for i, ok := range acquiredLocks {
		if !ok {
			// try a second time
			_, err := c.clients[i].AcquireRotateLock(ctx, request)
			if err != nil {
				log.Println(err)
				// try to release already acquired locks
				errRelease := c.releaseAcquiredLocks(ctx, acquiredLocks)
				if errRelease != nil {
					return false, fmt.Errorf("while trying to aquire lock on client %d: %s. While trying to release acquired locks: %s", i, err, errRelease)
				}
				// we could not acquire all the locks, but we successfully
				// released the acquired ones.
				// the controller will retry later
				return false, nil
			}
		}
	}
	return true, nil
}

func (c *shardedMultiGenerationController) callRotate(ctx context.Context) error {
	wg := sync.WaitGroup{}
	// call rotate on all the servers in parallel and keep track of who succeed
	okeys := make([]bool, len(c.clients))
	for i, client := range c.clients {
		wg.Add(1)
		go func(i int, client mg_proto.ShardedMultiGenerationControllerClient) {
			defer wg.Done()
			_, err := client.DoRotate(ctx, request)
			if err == nil {
				okeys[i] = true
			}
		}(i, client)
	}
	wg.Wait()

	// for failed calls, if any, we retry one last time
	for i, ok := range okeys {
		if !ok {
			_, err := c.clients[i].DoRotate(ctx, request)
			if err != nil {
				// TODO: try to recover by forcing a reset of the storage pods
				return err
			}
		}
	}
	return nil
}
