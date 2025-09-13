package server

import (
	"context"
	"time"

	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/ZephyrFS/zephyrfs-coordinator/internal/coordinator"
	"github.com/ZephyrFS/zephyrfs-coordinator/internal/models"
	pb "github.com/ZephyrFS/zephyrfs-proto/gen/go/coordinator"
)

// CoordinatorServer implements the gRPC CoordinatorService
type CoordinatorServer struct {
	pb.UnimplementedCoordinatorServiceServer
	coordinator *coordinator.Coordinator
}

// NewCoordinatorServer creates a new gRPC server instance
func NewCoordinatorServer(coord *coordinator.Coordinator) *CoordinatorServer {
	return &CoordinatorServer{
		coordinator: coord,
	}
}

// RegisterCoordinatorService registers the coordinator service with the gRPC server
func RegisterCoordinatorService(grpcServer *grpc.Server, coord *coordinator.Coordinator) {
	server := NewCoordinatorServer(coord)
	pb.RegisterCoordinatorServiceServer(grpcServer, server)
	logrus.Info("Coordinator gRPC service registered")
}

// RegisterNode handles node registration requests
func (s *CoordinatorServer) RegisterNode(ctx context.Context, req *pb.RegisterNodeRequest) (*pb.RegisterNodeResponse, error) {
	logrus.WithFields(logrus.Fields{
		"nodeID":    req.NodeId,
		"addresses": req.Addresses,
		"capacity":  req.StorageCapacity,
	}).Debug("Processing node registration")

	// Convert protobuf request to internal model
	modelReq := &models.RegisterNodeRequest{
		NodeID:          req.NodeId,
		Addresses:       req.Addresses,
		StorageCapacity: req.StorageCapacity,
		Capabilities:    req.Capabilities,
	}

	// Process the request
	resp, err := s.coordinator.RegisterNode(ctx, modelReq)
	if err != nil {
		logrus.WithError(err).Error("Failed to register node")
		return nil, status.Errorf(codes.Internal, "Failed to register node: %v", err)
	}

	// Convert internal response to protobuf
	pbResp := &pb.RegisterNodeResponse{
		Success:        resp.Success,
		Message:        resp.Message,
		AssignedNodeId: resp.AssignedNodeID,
		BootstrapPeers: resp.BootstrapPeers,
	}

	logrus.WithFields(logrus.Fields{
		"nodeID":         resp.AssignedNodeID,
		"bootstrapPeers": len(resp.BootstrapPeers),
	}).Info("Node registered successfully")

	return pbResp, nil
}

// UnregisterNode handles node unregistration requests
func (s *CoordinatorServer) UnregisterNode(ctx context.Context, req *pb.UnregisterNodeRequest) (*pb.UnregisterNodeResponse, error) {
	logrus.WithFields(logrus.Fields{
		"nodeID": req.NodeId,
		"reason": req.Reason,
	}).Debug("Processing node unregistration")

	modelReq := &models.UnregisterNodeRequest{
		NodeID: req.NodeId,
		Reason: req.Reason,
	}

	resp, err := s.coordinator.UnregisterNode(ctx, modelReq)
	if err != nil {
		logrus.WithError(err).Error("Failed to unregister node")
		return nil, status.Errorf(codes.Internal, "Failed to unregister node: %v", err)
	}

	return &pb.UnregisterNodeResponse{
		Success: resp.Success,
		Message: resp.Message,
	}, nil
}

// GetActiveNodes returns a list of active nodes
func (s *CoordinatorServer) GetActiveNodes(ctx context.Context, req *pb.GetActiveNodesRequest) (*pb.GetActiveNodesResponse, error) {
	logrus.WithFields(logrus.Fields{
		"limit":        req.Limit,
		"excludeNodes": len(req.ExcludeNodes),
	}).Debug("Processing get active nodes request")

	modelReq := &models.GetActiveNodesRequest{
		Limit:        req.Limit,
		ExcludeNodes: req.ExcludeNodes,
	}

	resp, err := s.coordinator.GetActiveNodes(ctx, modelReq)
	if err != nil {
		logrus.WithError(err).Error("Failed to get active nodes")
		return nil, status.Errorf(codes.Internal, "Failed to get active nodes: %v", err)
	}

	// Convert nodes to protobuf format
	var pbNodes []*pb.NodeStatus
	for _, node := range resp.Nodes {
		pbNode := &pb.NodeStatus{
			NodeId:        node.NodeID,
			Addresses:     node.Addresses,
			LastHeartbeat: node.LastHeartbeat,
			Status:        node.Status,
		}

		if node.Stats != nil {
			pbNode.Stats = &pb.NodeStats{
				StorageUsed:      node.Stats.StorageUsed,
				StorageAvailable: node.Stats.StorageAvailable,
				ChunksStored:     node.Stats.ChunksStored,
				BandwidthUp:      node.Stats.BandwidthUp,
				BandwidthDown:    node.Stats.BandwidthDown,
				CpuUsage:         node.Stats.CpuUsage,
				MemoryUsage:      node.Stats.MemoryUsage,
				UptimeSeconds:    node.Stats.UptimeSeconds,
			}
		}

		pbNodes = append(pbNodes, pbNode)
	}

	return &pb.GetActiveNodesResponse{
		Nodes:      pbNodes,
		TotalNodes: resp.TotalNodes,
	}, nil
}

// NodeHeartbeat processes heartbeat messages from nodes
func (s *CoordinatorServer) NodeHeartbeat(ctx context.Context, req *pb.NodeHeartbeatRequest) (*pb.NodeHeartbeatResponse, error) {
	// Log heartbeat at debug level to avoid spam
	logrus.WithField("nodeID", req.NodeId).Debug("Processing node heartbeat")

	modelReq := &models.NodeHeartbeatRequest{
		NodeID: req.NodeId,
	}

	if req.Stats != nil {
		modelReq.Stats = &models.NodeStats{
			StorageUsed:      req.Stats.StorageUsed,
			StorageAvailable: req.Stats.StorageAvailable,
			ChunksStored:     req.Stats.ChunksStored,
			BandwidthUp:      req.Stats.BandwidthUp,
			BandwidthDown:    req.Stats.BandwidthDown,
			CpuUsage:         req.Stats.CpuUsage,
			MemoryUsage:      req.Stats.MemoryUsage,
			UptimeSeconds:    req.Stats.UptimeSeconds,
		}
	}

	resp, err := s.coordinator.NodeHeartbeat(ctx, modelReq)
	if err != nil {
		logrus.WithError(err).WithField("nodeID", req.NodeId).Error("Failed to process heartbeat")
		return nil, status.Errorf(codes.Internal, "Failed to process heartbeat: %v", err)
	}

	return &pb.NodeHeartbeatResponse{
		Success: resp.Success,
		Message: resp.Message,
		Tasks:   resp.Tasks,
	}, nil
}

// RegisterFile handles file registration requests
func (s *CoordinatorServer) RegisterFile(ctx context.Context, req *pb.RegisterFileRequest) (*pb.RegisterFileResponse, error) {
	logrus.WithFields(logrus.Fields{
		"fileID":   req.FileId,
		"fileName": req.FileName,
		"fileSize": req.FileSize,
		"chunks":   len(req.Chunks),
	}).Debug("Processing file registration")

	// Convert chunks
	var chunks []*models.ChunkMetadata
	for _, chunk := range req.Chunks {
		chunks = append(chunks, &models.ChunkMetadata{
			ChunkID: chunk.ChunkId,
			Hash:    chunk.Hash,
			Size:    chunk.Size,
			Index:   chunk.Index,
		})
	}

	modelReq := &models.RegisterFileRequest{
		FileID:      req.FileId,
		FileName:    req.FileName,
		FileSize:    req.FileSize,
		FileHash:    req.FileHash,
		Chunks:      chunks,
		OwnerNodeID: req.OwnerNodeId,
	}

	resp, err := s.coordinator.RegisterFile(ctx, modelReq)
	if err != nil {
		logrus.WithError(err).Error("Failed to register file")
		return nil, status.Errorf(codes.Internal, "Failed to register file: %v", err)
	}

	// Convert chunk placements
	var pbPlacements []*pb.ChunkPlacement
	for _, placement := range resp.ChunkPlacements {
		pbPlacements = append(pbPlacements, &pb.ChunkPlacement{
			ChunkId:           placement.ChunkID,
			TargetNodes:       placement.TargetNodes,
			ReplicationFactor: placement.ReplicationFactor,
		})
	}

	return &pb.RegisterFileResponse{
		Success:         resp.Success,
		Message:         resp.Message,
		ChunkPlacements: pbPlacements,
	}, nil
}

// GetFileInfo retrieves information about a specific file
func (s *CoordinatorServer) GetFileInfo(ctx context.Context, req *pb.GetFileInfoRequest) (*pb.GetFileInfoResponse, error) {
	logrus.WithField("fileID", req.FileId).Debug("Processing get file info request")

	modelReq := &models.GetFileInfoRequest{
		FileID: req.FileId,
	}

	resp, err := s.coordinator.GetFileInfo(ctx, modelReq)
	if err != nil {
		logrus.WithError(err).Error("Failed to get file info")
		return nil, status.Errorf(codes.Internal, "Failed to get file info: %v", err)
	}

	pbResp := &pb.GetFileInfoResponse{
		Success: resp.Success,
		Message: resp.Message,
	}

	if resp.FileInfo != nil {
		// Convert chunks
		var pbChunks []*pb.ChunkRecord
		for _, chunk := range resp.FileInfo.Chunks {
			pbChunks = append(pbChunks, &pb.ChunkRecord{
				ChunkId:          chunk.ChunkID,
				Hash:             chunk.Hash,
				Size:             chunk.Size,
				Index:            chunk.Index,
				StoredAtNodes:    chunk.StoredAtNodes,
				ReplicationCount: chunk.ReplicationCount,
			})
		}

		pbResp.FileInfo = &pb.FileRecord{
			FileId:       resp.FileInfo.FileID,
			FileName:     resp.FileInfo.FileName,
			FileSize:     resp.FileInfo.FileSize,
			FileHash:     resp.FileInfo.FileHash,
			Chunks:       pbChunks,
			OwnerNodeId:  resp.FileInfo.OwnerNodeID,
			CreatedAt:    resp.FileInfo.CreatedAt,
			LastAccessed: resp.FileInfo.LastAccessed,
		}
	}

	return pbResp, nil
}

// UpdateChunkLocations updates where chunks are stored
func (s *CoordinatorServer) UpdateChunkLocations(ctx context.Context, req *pb.UpdateChunkLocationsRequest) (*pb.UpdateChunkLocationsResponse, error) {
	logrus.WithFields(logrus.Fields{
		"chunkID":   req.ChunkId,
		"nodeIDs":   req.NodeIds,
		"operation": req.Operation,
	}).Debug("Processing chunk locations update")

	modelReq := &models.UpdateChunkLocationsRequest{
		ChunkID:   req.ChunkId,
		NodeIDs:   req.NodeIds,
		Operation: req.Operation,
	}

	resp, err := s.coordinator.UpdateChunkLocations(ctx, modelReq)
	if err != nil {
		logrus.WithError(err).Error("Failed to update chunk locations")
		return nil, status.Errorf(codes.Internal, "Failed to update chunk locations: %v", err)
	}

	return &pb.UpdateChunkLocationsResponse{
		Success: resp.Success,
		Message: resp.Message,
	}, nil
}

// FindChunkLocations finds nodes that store a specific chunk
func (s *CoordinatorServer) FindChunkLocations(ctx context.Context, req *pb.FindChunkLocationsRequest) (*pb.FindChunkLocationsResponse, error) {
	logrus.WithFields(logrus.Fields{
		"chunkID":        req.ChunkId,
		"preferredCount": req.PreferredCount,
	}).Debug("Processing find chunk locations request")

	modelReq := &models.FindChunkLocationsRequest{
		ChunkID:        req.ChunkId,
		PreferredCount: req.PreferredCount,
	}

	resp, err := s.coordinator.FindChunkLocations(ctx, modelReq)
	if err != nil {
		logrus.WithError(err).Error("Failed to find chunk locations")
		return nil, status.Errorf(codes.Internal, "Failed to find chunk locations: %v", err)
	}

	return &pb.FindChunkLocationsResponse{
		Success:       resp.Success,
		Message:       resp.Message,
		NodeIds:       resp.NodeIDs,
		NodeAddresses: resp.NodeAddresses,
	}, nil
}

// GetNetworkStatus returns current network status and statistics
func (s *CoordinatorServer) GetNetworkStatus(ctx context.Context, req *pb.GetNetworkStatusRequest) (*pb.GetNetworkStatusResponse, error) {
	logrus.Debug("Processing get network status request")

	resp, err := s.coordinator.GetNetworkStatus(ctx)
	if err != nil {
		logrus.WithError(err).Error("Failed to get network status")
		return nil, status.Errorf(codes.Internal, "Failed to get network status: %v", err)
	}

	// Convert network stats
	var pbNetworkStats *pb.NetworkStats
	if resp.NetworkStats != nil {
		pbNetworkStats = &pb.NetworkStats{
			TotalNodes:             resp.NetworkStats.TotalNodes,
			ActiveNodes:            resp.NetworkStats.ActiveNodes,
			TotalStorageCapacity:   resp.NetworkStats.TotalStorageCapacity,
			TotalStorageUsed:       resp.NetworkStats.TotalStorageUsed,
			TotalFiles:             resp.NetworkStats.TotalFiles,
			TotalChunks:            resp.NetworkStats.TotalChunks,
			AverageNodeUptime:      resp.NetworkStats.AverageNodeUptime,
			NetworkUptimeSeconds:   resp.NetworkStats.NetworkUptimeSeconds,
		}
	}

	// Convert active nodes
	var pbActiveNodes []*pb.NodeStatus
	for _, node := range resp.ActiveNodes {
		pbNode := &pb.NodeStatus{
			NodeId:        node.NodeID,
			Addresses:     node.Addresses,
			LastHeartbeat: node.LastHeartbeat,
			Status:        node.Status,
		}

		if node.Stats != nil {
			pbNode.Stats = &pb.NodeStats{
				StorageUsed:      node.Stats.StorageUsed,
				StorageAvailable: node.Stats.StorageAvailable,
				ChunksStored:     node.Stats.ChunksStored,
				BandwidthUp:      node.Stats.BandwidthUp,
				BandwidthDown:    node.Stats.BandwidthDown,
				CpuUsage:         node.Stats.CpuUsage,
				MemoryUsage:      node.Stats.MemoryUsage,
				UptimeSeconds:    node.Stats.UptimeSeconds,
			}
		}

		pbActiveNodes = append(pbActiveNodes, pbNode)
	}

	return &pb.GetNetworkStatusResponse{
		NetworkStats: pbNetworkStats,
		ActiveNodes:  pbActiveNodes,
		Timestamp:    resp.Timestamp,
	}, nil
}

// LoggingInterceptor provides request logging for gRPC calls
func LoggingInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	start := time.Now()

	// Call the handler
	resp, err := handler(ctx, req)

	// Log the request
	duration := time.Since(start)
	fields := logrus.Fields{
		"method":   info.FullMethod,
		"duration": duration,
	}

	if err != nil {
		fields["error"] = err.Error()
		logrus.WithFields(fields).Error("gRPC request failed")
	} else {
		// Only log at debug level for successful requests to avoid spam
		if duration > 100*time.Millisecond {
			logrus.WithFields(fields).Info("gRPC request (slow)")
		} else {
			logrus.WithFields(fields).Debug("gRPC request completed")
		}
	}

	return resp, err
}