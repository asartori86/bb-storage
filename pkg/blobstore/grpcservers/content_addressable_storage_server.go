package grpcservers

import (
	"context"
	"fmt"
	"io"
	"log"
	"math"
	"os"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/justbuild"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type contentAddressableStorageServer struct {
	contentAddressableStorage blobstore.BlobAccess
	maximumMessageSizeBytes   int64
}

// NewContentAddressableStorageServer creates a GRPC service for serving
// the contents of a Bazel Content Addressable Storage (CAS) to Bazel.
func NewContentAddressableStorageServer(contentAddressableStorage blobstore.BlobAccess, maximumMessageSizeBytes int64) remoteexecution.ContentAddressableStorageServer {
	return &contentAddressableStorageServer{
		contentAddressableStorage: contentAddressableStorage,
		maximumMessageSizeBytes:   maximumMessageSizeBytes,
	}
}

func (s *contentAddressableStorageServer) FindMissingBlobs(ctx context.Context, in *remoteexecution.FindMissingBlobsRequest) (*remoteexecution.FindMissingBlobsResponse, error) {
	if len(in.BlobDigests) == 0 {
		return &remoteexecution.FindMissingBlobsResponse{}, nil
	}
	instanceName, err := digest.NewInstanceName(in.InstanceName)
	if err != nil {
		return nil, util.StatusWrapf(err, "Invalid instance name %#v", in.InstanceName)
	}
	digestFunction, err := instanceName.GetDigestFunction(in.DigestFunction, len(in.BlobDigests[0].GetHash()))
	if err != nil {
		return nil, err
	}

	inDigests := digest.NewSetBuilder()
	for _, partialDigest := range in.BlobDigests {
		digest, err := digestFunction.NewDigestFromProto(partialDigest)
		if err != nil {
			return nil, err
		}
		inDigests.Add(digest)
	}
	outDigests, err := s.contentAddressableStorage.FindMissing(ctx, inDigests.Build())
	if err != nil {
		return nil, err
	}
	partialDigests := make([]*remoteexecution.Digest, 0, outDigests.Length())
	for _, outDigest := range outDigests.Items() {
		partialDigests = append(partialDigests, outDigest.GetProto())
	}
	return &remoteexecution.FindMissingBlobsResponse{
		MissingBlobDigests: partialDigests,
	}, nil
}

func (s *contentAddressableStorageServer) BatchReadBlobs(ctx context.Context, in *remoteexecution.BatchReadBlobsRequest) (*remoteexecution.BatchReadBlobsResponse, error) {
	if len(in.Digests) == 0 {
		return &remoteexecution.BatchReadBlobsResponse{}, nil
	}
	instanceName, err := digest.NewInstanceName(in.InstanceName)
	if err != nil {
		return nil, util.StatusWrapf(err, "Invalid instance name %#v", in.InstanceName)
	}
	digestFunction, err := instanceName.GetDigestFunction(in.DigestFunction, len(in.Digests[0].GetHash()))
	if err != nil {
		return nil, err
	}

	bytesRemaining := s.maximumMessageSizeBytes
	digests := make([]digest.Digest, 0, len(in.Digests))
	for _, reqDigest := range in.Digests {
		digest, err := digestFunction.NewDigestFromProto(reqDigest)
		if err != nil {
			return nil, err
		}
		sizeBytes := digest.GetSizeBytes()
		if sizeBytes > bytesRemaining {
			return nil, status.Errorf(
				codes.InvalidArgument,
				"Attempted to read a total of at least %d bytes, while a maximum of %d bytes is permitted",
				uint64(s.maximumMessageSizeBytes-bytesRemaining)+uint64(sizeBytes),
				s.maximumMessageSizeBytes)
		}
		bytesRemaining -= sizeBytes
		digests = append(digests, digest)
	}

	response := &remoteexecution.BatchReadBlobsResponse{
		Responses: make([]*remoteexecution.BatchReadBlobsResponse_Response, 0, len(in.Digests)),
	}
	for i, reqDigest := range in.Digests {
		data, err := s.contentAddressableStorage.Get(
			ctx,
			digests[i]).ToByteSlice(int(digests[i].GetSizeBytes()))
		response.Responses = append(response.Responses, &remoteexecution.BatchReadBlobsResponse_Response{
			Digest: reqDigest,
			Data:   data,
			Status: status.Convert(err).Proto(),
		})
	}

	return response, nil
}

func (s *contentAddressableStorageServer) BatchUpdateBlobs(ctx context.Context, in *remoteexecution.BatchUpdateBlobsRequest) (*remoteexecution.BatchUpdateBlobsResponse, error) {
	if len(in.Requests) == 0 {
		return &remoteexecution.BatchUpdateBlobsResponse{}, nil
	}
	instanceName, err := digest.NewInstanceName(in.InstanceName)
	if err != nil {
		return nil, util.StatusWrapf(err, "Invalid instance name %#v", in.InstanceName)
	}
	digestFunction, err := instanceName.GetDigestFunction(in.DigestFunction, len(in.Requests[0].Digest.GetHash()))
	if err != nil {
		return nil, err
	}

	response := &remoteexecution.BatchUpdateBlobsResponse{
		Responses: make([]*remoteexecution.BatchUpdateBlobsResponse_Response, 0, len(in.Requests)),
	}
	for _, request := range in.Requests {
		digest, err := digestFunction.NewDigestFromProto(request.Digest)
		if err == nil {
			err = s.contentAddressableStorage.Put(
				ctx,
				digest,
				buffer.NewCASBufferFromByteSlice(digest, request.Data, buffer.UserProvided))
		}
		response.Responses = append(response.Responses,
			&remoteexecution.BatchUpdateBlobsResponse_Response{
				Digest: request.Digest,
				Status: status.Convert(err).Proto(),
			})
	}
	return response, nil
}

func (s *contentAddressableStorageServer) GetTree(in *remoteexecution.GetTreeRequest, stream remoteexecution.ContentAddressableStorage_GetTreeServer) error {
	return status.Error(codes.Unimplemented, "This service does not support downloading directory trees")
}

func (s *contentAddressableStorageServer) SplitBlob(ctx context.Context, in *remoteexecution.SplitBlobRequest) (*remoteexecution.SplitBlobResponse, error) {
	if in.BlobDigest == nil {
		err := status.Error(codes.InvalidArgument, "SplitBlob: no blob digest provided")
		log.Println(err)
		return nil, err
	}
	log.Printf("SplitBlob(%s, %s)", in.BlobDigest.GetHash(), remoteexecution.ChunkingAlgorithm_Value_name[int32(in.ChunkingAlgorithm)])
	if in.ChunkingAlgorithm != remoteexecution.ChunkingAlgorithm_IDENTITY && in.ChunkingAlgorithm != remoteexecution.ChunkingAlgorithm_FASTCDC_MT0_8KB {
		log.Println("SplitBlob: unsupported chunking algorithm %s, will use default implementation %s",
			remoteexecution.ChunkingAlgorithm_Value_name[int32(in.ChunkingAlgorithm)],
			remoteexecution.ChunkingAlgorithm_Value_name[int32(remoteexecution.ChunkingAlgorithm_FASTCDC_MT0_8KB)])
	}
	instanceName, err := digest.NewInstanceName(in.InstanceName)
	if err != nil {
		err := util.StatusWrapf(err, "SplitBlob: invalid instance name %#v", in.InstanceName)
		log.Println(err)
		return nil, err
	}
	digestFunction, err := instanceName.GetDigestFunction(remoteexecution.DigestFunction_UNKNOWN, len(in.BlobDigest.GetHash()))
	if err != nil {
		err := util.StatusWrapf(err, "SplitBlob: invalid digest length %d", len(in.BlobDigest.GetHash()))
		log.Println(err)
		return nil, err
	}
	blobDigest, err := digestFunction.NewDigestFromProto(in.BlobDigest)
	if err != nil {
		err := util.StatusWrap(err, "SplitBlob: digest generation from proto message failed")
		log.Println(err)
		return nil, err
	}
	// Check blob existence.
	inDigests := digest.NewSetBuilder()
	inDigests.Add(blobDigest)
	ctxWithCancel, cancel := context.WithCancel(ctx)
	outDigests, err := s.contentAddressableStorage.FindMissing(ctxWithCancel, inDigests.Build())
	if !outDigests.Empty() {
		cancel()
		err := status.Errorf(codes.NotFound, "SplitBlob: blob not found %s", blobDigest.GetHashString())
		log.Println(err)
		return nil, err
	}
	// Handle chunking algorithms.
	chunkDigests := []*remoteexecution.Digest{}
	if in.ChunkingAlgorithm == remoteexecution.ChunkingAlgorithm_IDENTITY {
		if justbuild.IsJustbuildTree(blobDigest.GetHashString()) {
			ctxWithCancel, cancel := context.WithCancel(ctx)
			treeContent, err := s.contentAddressableStorage.Get(ctxWithCancel, blobDigest).ToByteSlice(math.MaxInt32)
			if err != nil {
				cancel()
				err := util.StatusWrap(err, "SplitBlob: could not read tree data")
				log.Println(err)
				return nil, err
			}
			generator := digestFunction.NewGenerator(math.MaxInt64)
			_, err = generator.Write(treeContent)
			if err != nil {
				err := util.StatusWrap(err, "SplitBlob: writing tree data into the digest generator failed")
				log.Println(err)
				return nil, err
			}
			treeDigest := generator.Sum()
			ctxWithCancel, cancel = context.WithCancel(ctx)
			err = s.contentAddressableStorage.Put(
				ctxWithCancel,
				treeDigest,
				buffer.NewCASBufferFromByteSlice(treeDigest, treeContent, buffer.UserProvided))
			if err != nil {
				cancel()
				err := util.StatusWrapf(err, "SplitBlob: storing tree as blob failed %s", treeDigest.GetHashString())
				log.Println(err)
				return nil, err
			}
			chunkDigests = append(chunkDigests, treeDigest.GetProto())
		} else {
			chunkDigests = append(chunkDigests, blobDigest.GetProto())
		}
	} else {
		ctxWithCancel, _ = context.WithCancel(ctx)
		blobReader := s.contentAddressableStorage.Get(ctxWithCancel, blobDigest).ToReader()
		// Split blob into chunks, store each chunk in CAS, and collect chunk
		// digests.
		chunker := NewBlobChunker(blobReader, DefaultChunkSize)
		for {
			chunk, err := chunker.NextChunk()
			if err == io.EOF {
				break
			}
			if err != nil {
				err := util.StatusWrap(err, "SplitBlob: determining next chunk failed")
				log.Println(err)
				return nil, err
			}
			generator := digestFunction.NewGenerator(int64(chunker.maxChunkSize))
			_, err = generator.Write(chunk)
			if err != nil {
				err := util.StatusWrap(err, "SplitBlob: writing chunk into the digest generator failed")
				log.Println(err)
				return nil, err
			}
			chunkDigest := generator.Sum()
			ctxWithCancel, cancel := context.WithCancel(ctx)
			err = s.contentAddressableStorage.Put(
				ctxWithCancel,
				chunkDigest,
				buffer.NewCASBufferFromByteSlice(chunkDigest, chunk, buffer.UserProvided))
			if err != nil {
				cancel()
				err := util.StatusWrapf(err, "SplitBlob: storing of chunk failed %s", chunkDigest.GetHashString())
				log.Println(err)
				return nil, err
			}
			chunkDigests = append(chunkDigests, chunkDigest.GetProto())
		}
	}
	str := fmt.Sprintf("Split blob %s:%d into [ ", blobDigest.GetHashString(), blobDigest.GetSizeBytes())
	for _, chunkDigest := range chunkDigests {
		str += fmt.Sprintf("%s:%d ", chunkDigest.GetHash(), chunkDigest.GetSizeBytes())
	}
	str += "]"
	log.Println(str)
	response := &remoteexecution.SplitBlobResponse{
		ChunkDigests: chunkDigests,
	}
	return response, nil
}

func (s *contentAddressableStorageServer) SpliceBlob(ctx context.Context, in *remoteexecution.SpliceBlobRequest) (*remoteexecution.SpliceBlobResponse, error) {
	if in.BlobDigest == nil {
		err := status.Error(codes.InvalidArgument, "SpliceBlob: no blob digest provided")
		log.Println(err)
		return nil, err
	}
	log.Printf("SpliceBlob(%s, %d chunks)", in.BlobDigest.GetHash(), len(in.ChunkDigests))
	instanceName, err := digest.NewInstanceName(in.InstanceName)
	if err != nil {
		err := util.StatusWrapf(err, "SpliceBlob: invalid instance name %#v", in.InstanceName)
		log.Println(err)
		return nil, err
	}
	digestFunction, err := instanceName.GetDigestFunction(remoteexecution.DigestFunction_UNKNOWN, len(in.BlobDigest.GetHash()))
	if err != nil {
		err := util.StatusWrapf(err, "SpliceBlob: invalid digest length %d", len(in.BlobDigest.GetHash()))
		log.Println(err)
		return nil, err
	}
	blobDigest, err := digestFunction.NewDigestFromProto(in.BlobDigest)
	if err != nil {
		err := util.StatusWrap(err, "SpliceBlob: digest generation from proto message failed")
		log.Println(err)
		return nil, err
	}
	// Assemble blob from chunks using a temp file.
	tmpFile, err := os.CreateTemp("", "blob") // default temp directory is used, a random string is added to "blob"
	if err != nil {
		err := util.StatusWrapf(err, "SpliceBlob: temp file could not be created")
		log.Println(err)
		return nil, err
	}
	defer os.Remove(tmpFile.Name())
	var generator *digest.Generator
	if justbuild.IsJustbuildTree(blobDigest.GetHashString()) {
		generator = digestFunction.NewTreeGenerator(math.MaxInt64)
	} else {
		generator = digestFunction.NewGenerator(math.MaxInt64)
	}
	for _, chunkDigestProto := range in.ChunkDigests {
		chunkDigest, err := digestFunction.NewDigestFromProto(chunkDigestProto)
		if err != nil {
			err := util.StatusWrap(err, "SpliceBlob: digest generation from proto message failed")
			log.Println(err)
			return nil, err
		}
		// Check chunk existence.
		inDigests := digest.NewSetBuilder()
		inDigests.Add(chunkDigest)
		ctxWithCancel, cancel := context.WithCancel(ctx)
		outDigests, err := s.contentAddressableStorage.FindMissing(ctxWithCancel, inDigests.Build())
		if !outDigests.Empty() {
			cancel()
			err := status.Errorf(codes.NotFound, "SpliceBlob: chunk not found %s", chunkDigest.GetHashString())
			log.Println(err)
			return nil, err
		}
		// Load chunk data and append to temp file.
		ctxWithCancel, _ = context.WithCancel(ctx)
		chunkBuffer, chunkBufferCopy := s.contentAddressableStorage.Get(ctxWithCancel, chunkDigest).CloneCopy(math.MaxInt32)
		err = chunkBuffer.IntoWriter(tmpFile)
		if err != nil {
			err := util.StatusWrap(err, "SpliceBlob: could not write chunk into temp file")
			log.Println(err)
			return nil, err
		}
		chunk, err := chunkBufferCopy.ToByteSlice(math.MaxInt32)
		if err != nil {
			err := util.StatusWrap(err, "SpliceBlob: creating slice from chunk buffer failed")
			log.Println(err)
			return nil, err
		}
		_, err = generator.Write(chunk)
		if err != nil {
			err := util.StatusWrap(err, "SpliceBlob: writing chunk into the digest generator failed")
			log.Println(err)
			return nil, err
		}
	}
	err = tmpFile.Close()
	if err != nil {
		err := util.StatusWrap(err, "SpliceBlob: could not successfully close temp file")
		log.Println(err)
		return nil, err
	}
	// Check digest consistency.
	computedBlobDigest := generator.Sum()
	compatibleMode := digestFunction.GetEnumValue() != remoteexecution.DigestFunction_GITSHA1
	if blobDigest.GetHashString() != computedBlobDigest.GetHashString() || ((compatibleMode || blobDigest.GetSizeBytes() > 0) && (blobDigest.GetSizeBytes() != computedBlobDigest.GetSizeBytes())) {
		err := status.Errorf(codes.InvalidArgument, "SpliceBlob: provided digest %s:%d and computed digest %s:%d do not correspond.", blobDigest.GetHashString(), blobDigest.GetSizeBytes(), computedBlobDigest.GetHashString(), computedBlobDigest.GetHashBytes())
		log.Println(err)
		return nil, err
	}
	// Store temp file in CAS as blob.
	ctxWithCancel, cancel := context.WithCancel(ctx)
	err = s.contentAddressableStorage.Put(
		ctxWithCancel,
		blobDigest,
		buffer.NewCASBufferFromReader(blobDigest, tmpFile, buffer.UserProvided))
	if err != nil {
		cancel()
		err := util.StatusWrapf(err, "SpliceBlob: could not store blob %s", blobDigest.GetHashString())
		log.Println(err)
		return nil, err
	}
	response := &remoteexecution.SpliceBlobResponse{
		BlobDigest: computedBlobDigest.GetProto(),
	}
	return response, nil
}
