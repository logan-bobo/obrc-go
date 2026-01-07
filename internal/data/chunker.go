package data

type ChunkInfo struct {
	Start int64
	End   int64
}

func buildChunks(fileSize int64, numChunks int) []ChunkInfo {
	chunkSize := fileSize / int64(numChunks)

	chunks := make([]ChunkInfo, numChunks)

	for i := range numChunks {
		start := int64(i) * chunkSize
		end := start + chunkSize

		chunks[i].Start = start
		chunks[i].End = end
	}

	// for the case where we have say 100 / 3 each chunk would be 33 and the
	// last chunk would end at byte 99 so make the final chunk go to the
	// end of the file
	chunks[numChunks-1].End = fileSize

	return chunks
}

type Chunker struct {
	Chunks []ChunkInfo
}

func NewChunker(fileSize int64, numChunks int) *Chunker {
	return &Chunker{
		Chunks: buildChunks(fileSize, numChunks),
	}
}
