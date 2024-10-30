// Package compressor tests
// BSD 3-Clause License
//
// Copyright (c) 2024, Alex Gaetano Padula
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:
//
//  1. Redistributions of source code must retain the above copyright notice, this
//     list of conditions and the following disclaimer.
//
//  2. Redistributions in binary form must reproduce the above copyright notice,
//     this list of conditions and the following disclaimer in the documentation
//     and/or other materials provided with the distribution.
//
//  3. Neither the name of the copyright holder nor the names of its
//     contributors may be used to endorse or promote products derived from
//     this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
// AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
// IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
// DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
// FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
// DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
// SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
// CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
// OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
package compressor

import (
	"bytes"
	"encoding/gob"
	"os"
	"testing"
)

func TestNewCompressor(t *testing.T) {
	tests := []struct {
		windowSize int
		expectErr  bool
	}{
		{windowSize: 32, expectErr: false},
		{windowSize: 0, expectErr: true},
		{windowSize: -1, expectErr: true},
	}

	for _, tt := range tests {
		_, err := NewCompressor(tt.windowSize)
		if (err != nil) != tt.expectErr {
			t.Errorf("NewCompressor(%d) error = %v, expectErr %v", tt.windowSize, err, tt.expectErr)
		}
	}
}

func TestCompressor_Compress(t *testing.T) {
	compressor, _ := NewCompressor(32)

	tests := []struct {
		data     []byte
		expected []byte
	}{
		{data: []byte{}, expected: []byte{}},
		{data: []byte("abcdef"), expected: []byte{0, 0, 'a', 0, 0, 'b', 0, 0, 'c', 0, 0, 'd', 0, 0, 'e', 0, 0, 'f'}},
		{data: []byte("aaaaaa"), expected: []byte{0, 0, 'a', 0, 0, 'a', 0, 0, 'a', 0, 0, 'a', 0, 0, 'a', 0, 0, 'a'}},
	}

	for _, tt := range tests {
		compressed := compressor.Compress(tt.data)
		if !bytes.Equal(compressed, tt.expected) {
			t.Errorf("Compress(%v) = %v, expected %v", tt.data, compressed, tt.expected)
		}
	}
}

func TestCompressor_Decompress(t *testing.T) {
	compressor, _ := NewCompressor(32)

	tests := []struct {
		data     []byte
		expected []byte
	}{
		{data: []byte{}, expected: []byte{}},
		{data: []byte{0, 0, 'a', 0, 0, 'b', 0, 0, 'c', 0, 0, 'd', 0, 0, 'e', 0, 0, 'f'}, expected: []byte("abcdef")},
		{data: []byte{0, 0, 'a', 0, 0, 'a', 0, 0, 'a', 0, 0, 'a', 0, 0, 'a', 0, 0, 'a'}, expected: []byte("aaaaaa")},
	}

	for _, tt := range tests {
		decompressed := compressor.Decompress(tt.data)
		if !bytes.Equal(decompressed, tt.expected) {
			t.Errorf("Decompress(%v) = %v, expected %v", tt.data, decompressed, tt.expected)
		}
	}
}

type TestStruct struct {
	Data []byte
	N    int64
	F    float64
	B    bool
}

func TestCompressor_CompressDecompress(t *testing.T) {
	compressor, _ := NewCompressor(32)

	tests := [][]byte{
		[]byte{},
		[]byte("abcdef"),
		[]byte("aaaaaa"),
		[]byte("abcabcabcabcabcabc"),
	}

	// We will encode a test struct
	testStruct := TestStruct{
		Data: []byte("Contrary to popular belief, Lorem Ipsum is not simply random text. It has roots in a piece of classical Latin literature from 45 BC, making it over 2000 years old. Richard McClintock, a Latin professor at Hampden-Sydney College in Virginia, looked up one of the more obscure Latin words, consectetur, from a Lorem Ipsum passage, and going through the cites of the word in classical literature, discovered the undoubtable source. Lorem Ipsum comes from sections 1.10.32 and 1.10.33 of \"de Finibus Bonorum et Malorum\" (The Extremes of Good and Evil) by Cicero, written in 45 BC. This book is a treatise on the theory of ethics, very popular during the Renaissance. The first line of Lorem Ipsum, \"Lorem ipsum dolor sit amet..\", comes from a line in section 1.10.32.\n\nThe standard chunk of Lorem Ipsum used since the 1500s is reproduced below for those interested. Sections 1.10.32 and 1.10.33 from \"de Finibus Bonorum et Malorum\" by Cicero are also reproduced in their exact original form, accompanied by English versions from the 1914 translation by H. Rackham."),
		N:    42,
		F:    3.14,
		B:    true,
	}

	buff := new(bytes.Buffer)

	// Encode
	encoder := gob.NewEncoder(buff)

	err := encoder.Encode(testStruct)
	if err != nil {
		t.Fatalf("Failed to encode test struct: %v", err)
	}

	tests = append(tests, buff.Bytes())

	for i, tt := range tests {
		compressed := compressor.Compress(tt)
		decompressed := compressor.Decompress(compressed)

		// check if last test, if so is struct test, decode it
		if i == len(tests)-1 {
			// Decode
			testStruct2 := TestStruct{}

			decoder := gob.NewDecoder(bytes.NewReader(decompressed))

			err := decoder.Decode(&testStruct2)
			if err != nil {
				t.Fatalf("Failed to decode test struct: %v", err)
			}

			if testStruct2.N != testStruct.N {
				t.Errorf("CompressDecompress(%v) = %v, expected %v", tt, testStruct2.N, testStruct.N)
			}

			if testStruct2.F != testStruct.F {
				t.Errorf("CompressDecompress(%v) = %v, expected %v", tt, testStruct2.F, testStruct.F)
			}

			if testStruct2.B != testStruct.B {
				t.Errorf("CompressDecompress(%v) = %v, expected %v", tt, testStruct2.B, testStruct.B)
			}
		} else {

			if !bytes.Equal(decompressed, tt) {
				t.Errorf("CompressDecompress(%v) = %v, expected %v", tt, decompressed, tt)
			}
		}
	}
}

// We test compressing multiple files and getting % of compression
func TestCompressor_Compression_Ratios(t *testing.T) {
	// We read test.png into memory and calculate the compression ratio

	// Read test.png
	data, err := os.ReadFile("test.png")
	if err != nil {
		t.Fatalf("Failed to read test.png: %v", err)
	}

	// Read test2_public_domain.jpg
	data2, err := os.ReadFile("test2_public_domain.jpg")
	if err != nil {
		t.Fatalf("Failed to read test2_public_domain.jpg: %v", err)
	}

	compressor, _ := NewCompressor(1024 * 32)

	compressed := compressor.Compress(data)

	// Calculate compression ratio
	compressionRatio := float64(len(compressed)) / float64(len(data))

	t.Logf("test.png compression ratio: %.2f times smaller than original", compressionRatio)

	compressor, _ = NewCompressor(1024 * 32)

	compressed = compressor.Compress(data2)

	// Calculate compression ratio
	compressionRatio = float64(len(compressed)) / float64(len(data2))

	t.Logf("test2_public_domain.jpg compression ratio: %.2f times smaller than original", compressionRatio)

}
