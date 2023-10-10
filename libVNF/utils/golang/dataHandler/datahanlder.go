package datahandler

import (
	"encoding/binary"
)

type DSPacketHandler struct {
	Data    []byte
	DataPtr int
	Len     int
}

func (dh *DSPacketHandler) ExtractItem(item interface{}) {
	switch v := item.(type) {
	case *int:
		itemLen := 4
		*v = int(binary.LittleEndian.Uint32(dh.Data[dh.DataPtr:]))
		dh.DataPtr += itemLen
	case *uint8:
		itemLen := 1
		*v = dh.Data[dh.DataPtr]
		dh.DataPtr += itemLen
	case *uint16:
		itemLen := 2
		*v = binary.LittleEndian.Uint16(dh.Data[dh.DataPtr:])
		dh.DataPtr += itemLen
	case *uint32:
		itemLen := 4
		*v = binary.LittleEndian.Uint32(dh.Data[dh.DataPtr:])
		dh.DataPtr += itemLen
	case *uint64:
		itemLen := 8
		*v = binary.LittleEndian.Uint64(dh.Data[dh.DataPtr:])
		dh.DataPtr += itemLen
	case *[]uint64:
		itemSize := int(binary.Size(v))
		itemEleLen := binary.Size(&(*v)[0])

		*v = make([]uint64, itemSize)
		for i := 0; i < itemSize; i++ {
			(*v)[i] = binary.LittleEndian.Uint64(dh.Data[dh.DataPtr:])
			dh.DataPtr += itemEleLen
		}
		panic("unfinished")
	// This case is used to help construct string.
	case *[]byte:
		itemLen := len(*v)
		copy(*v, dh.Data[dh.DataPtr:])
		dh.DataPtr += itemLen
	case *string:
		var itemLen int
		dh.ExtractItem(&itemLen)

		citem := make([]byte, itemLen)
		dh.ExtractItem(&citem)
		*v = string(citem)
	}
}

func (dh *DSPacketHandler) AppendItem(item interface{}) {
	switch v := item.(type) {
	// case bool:
	// 	itemLen := 1
	// 	binary.LittleEndian.(dh.Data[dh.DataPtr:], v)
	// 	dh.DataPtr += itemLen
	// 	dh.Len += itemLen
	case int:
		itemLen := 4
		binary.LittleEndian.PutUint32(dh.Data[dh.DataPtr:], uint32(v))
		dh.DataPtr += itemLen
		dh.Len += itemLen
	case []uint64:
		itemEleLen := binary.Size(v[0])
		itemSize := len(v)

		for i := 0; i < itemSize; i++ {
			binary.LittleEndian.PutUint64(dh.Data[dh.DataPtr:], v[i])
			dh.DataPtr += itemEleLen
			dh.Len += itemEleLen
		}
	case []byte:
		itemLen := len(v)
		copy(dh.Data[dh.DataPtr:], v)
		dh.DataPtr += itemLen
		dh.Len += itemLen
	case string:
		itemLen := len(v)
		dh.AppendItem(itemLen)
		dh.AppendItem([]byte(v))
	default:
		panic("unimplemented.")
	}
}

func (dh *DSPacketHandler) PrependLength() {
	binary.LittleEndian.PutUint32(dh.Data[0:], uint32(dh.Len))
}

// func main() {
// 	// Create a DSPacketHandler instance and populate the Data

// 	// Example usage of extractItem:
// 	var intItem int
// 	dh.extractItem(&intItem)

// 	var uint8Item uint8
// 	dh.extractItem(&uint8Item)

// 	var uint16Item uint16
// 	dh.extractItem(&uint16Item)

// 	var uint32Item uint32
// 	dh.extractItem(&uint32Item)

// 	var uint64Item uint64
// 	dh.extractItem(&uint64Item)

// 	var uint64Slice []uint64
// 	dh.extractItem(&uint64Slice, Len(uint64Slice))

// 	var byteSlice []byte
// 	dh.extractItem(&byteSlice, Len(byteSlice))

// 	var stringItem string
// 	dh.extractItem(&stringItem)
// }
