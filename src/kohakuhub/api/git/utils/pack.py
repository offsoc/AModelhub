"""Pure Python Git pack file parser.

This module provides functionality to decode Git pack files (version 2)
without any native dependencies. It supports base objects and delta compression
(OFS_DELTA and REF_DELTA).
"""

import hashlib
import struct
import zlib
from typing import BinaryIO, Dict, List, Optional, Tuple, Any


class GitPackParser:
    """Parser for Git pack files (v2)."""

    def __init__(self, pack_data: bytes):
        self.data = pack_data
        self.offset = 0
        self.objects = {}  # sha1 -> (type, content)
        self.offsets = {} # offset -> (type, content)

    def parse(self) -> Dict[str, Tuple[int, bytes]]:
        """Parse the pack file and return objects.

        Returns:
            Dictionary mapping SHA-1 hex to (object_type, content)
        """
        # 1. Parse Header
        signature = self.data[self.offset : self.offset + 4]
        if signature != b"PACK":
            raise ValueError(f"Invalid pack signature: {signature}")
        self.offset += 4

        version = struct.unpack(">I", self.data[self.offset : self.offset + 4])[0]
        if version != 2:
            raise ValueError(f"Unsupported pack version: {version}")
        self.offset += 4

        num_objects = struct.unpack(">I", self.data[self.offset : self.offset + 4])[0]
        self.offset += 4

        # 2. Parse Objects
        for _ in range(num_objects):
            obj_offset = self.offset
            obj_type, obj_size, data_start = self._read_type_and_size()
            
            content = None
            if obj_type in (1, 2, 3, 4):
                # Base object
                content = self._read_base_object(obj_size)
            elif obj_type == 6:
                # OFS_DELTA
                content = self._read_ofs_delta(obj_size, obj_offset)
            elif obj_type == 7:
                # REF_DELTA
                content = self._read_ref_delta(obj_size)
            else:
                raise ValueError(f"Unknown object type: {obj_type}")

            # Compute SHA-1 and store
            obj_sha1 = self._compute_sha1(obj_type, content)
            self.objects[obj_sha1] = (obj_type, content)
            self.offsets[obj_offset] = (obj_type, content)

        # 3. Verify Checksum (optional but good)
        actual_checksum = hashlib.sha1(self.data[: self.offset]).digest()
        expected_checksum = self.data[self.offset : self.offset + 20]
        # if actual_checksum != expected_checksum:
        #    raise ValueError("Pack checksum mismatch")

        return self.objects

    def _read_type_and_size(self) -> Tuple[int, int, int]:
        """Read object type and size from variable-length encoding."""
        byte = self.data[self.offset]
        self.offset += 1
        
        obj_type = (byte >> 4) & 7
        size = byte & 15
        shift = 4
        
        while byte & 128:
            byte = self.data[self.offset]
            self.offset += 1
            size += (byte & 127) << shift
            shift += 7
            
        return obj_type, size, self.offset

    def _read_base_object(self, size: int) -> bytes:
        """Read and decompress base object."""
        decompressor = zlib.decompressobj()
        content = decompressor.decompress(self.data[self.offset :])
        self.offset += len(self.data[self.offset :]) - len(decompressor.unused_data)
        
        if len(content) != size:
            raise ValueError(f"Decompressed size mismatch: expected {size}, got {len(content)}")
            
        return content

    def _read_ofs_delta(self, size: int, obj_offset: int) -> bytes:
        """Read and apply offset-based delta."""
        # Read offset encoding
        byte = self.data[self.offset]
        self.offset += 1
        rel_offset = byte & 127
        while byte & 128:
            byte = self.data[self.offset]
            self.offset += 1
            rel_offset = ((rel_offset + 1) << 7) | (byte & 127)
            
        base_offset = obj_offset - rel_offset
        base_type, base_content = self.offsets[base_offset]
        
        # Read delta data
        delta_data = self._read_base_object(size) # Compressed delta
        
        return self._apply_delta(base_content, delta_data)

    def _read_ref_delta(self, size: int) -> bytes:
        """Read and apply SHA1-based delta."""
        base_sha1 = self.data[self.offset : self.offset + 20].hex()
        self.offset += 20
        
        if base_sha1 not in self.objects:
             # We should probably have this object from previous packs or LakeFS
             # For now, let's assume it's in the same pack
             raise ValueError(f"Base object {base_sha1} not found in pack")
             
        base_type, base_content = self.objects[base_sha1]
        delta_data = self._read_base_object(size)
        
        return self._apply_delta(base_content, delta_data)

    def _apply_delta(self, base_content: bytes, delta_data: bytes) -> bytes:
        """Apply delta instructions to base content."""
        def read_var_int(data, pos):
            result = 0
            shift = 0
            while True:
                byte = data[pos]
                pos += 1
                result |= (byte & 127) << shift
                if not (byte & 128):
                    return result, pos
                shift += 7

        pos = 0
        src_size, pos = read_var_int(delta_data, pos)
        dst_size, pos = read_var_int(delta_data, pos)
        
        if src_size != len(base_content):
            raise ValueError(f"Delta source size mismatch: {src_size} vs {len(base_content)}")
            
        result = bytearray()
        while pos < len(delta_data):
            opcode = delta_data[pos]
            pos += 1
            
            if opcode & 128:
                # Copy from base
                offset = 0
                size = 0
                
                # Build offset
                if opcode & 0x01: offset |= delta_data[pos]; pos += 1
                if opcode & 0x02: offset |= delta_data[pos] << 8; pos += 1
                if opcode & 0x04: offset |= delta_data[pos] << 16; pos += 1
                if opcode & 0x08: offset |= delta_data[pos] << 24; pos += 1
                
                # Build size
                if opcode & 0x10: size |= delta_data[pos]; pos += 1
                if opcode & 0x20: size |= delta_data[pos] << 8; pos += 1
                if opcode & 0x40: size |= delta_data[pos] << 16; pos += 1
                
                if size == 0: size = 0x10000
                
                result.extend(base_content[offset : offset + size])
            elif opcode > 0:
                # Copy from delta (add data)
                result.extend(delta_data[pos : pos + opcode])
                pos += opcode
            else:
                raise ValueError("Invalid delta opcode 0")
                
        if len(result) != dst_size:
            raise ValueError(f"Delta result size mismatch: {len(result)} vs {dst_size}")
            
        return bytes(result)

    def _compute_sha1(self, obj_type: int, content: bytes) -> str:
        """Compute Git SHA-1 for an object."""
        type_str = {1: "commit", 2: "tree", 3: "blob", 4: "tag"}.get(obj_type, "blob")
        header = f"{type_str} {len(content)}\0".encode()
        return hashlib.sha1(header + content).hexdigest()
