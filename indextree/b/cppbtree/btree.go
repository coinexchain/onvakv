// +build cppbtree

package cppbtree

/*
#cgo CXXFLAGS: -O3 -std=c++11 
#cgo LDFLAGS: -lstdc++
#include "cppbtree.h"
*/
import "C"

import (
	"io"
	"unsafe"
)

type Enumerator struct {
	tree    unsafe.Pointer
	iter    unsafe.Pointer
}

type Tree struct {
	ptr   unsafe.Pointer
}

func TreeNew(_ func(a, b []byte) int) *Tree {
	return &Tree{
		ptr: C.cppbtree_new(),
	}
}

func (tree *Tree) Close() {
	C.cppbtree_delete(tree.ptr);
}

func (tree *Tree) PutNewAndGetOld(key []byte, newV uint64) (uint64, bool) {
	keydata := (*C.char)(unsafe.Pointer(&key[0]))
	var oldExists C.int
	oldV := C.cppbtree_put_new_and_get_old(tree.ptr, keydata, C.int(len(key)), C.ulonglong(newV), &oldExists)
	return uint64(oldV), oldExists!=0
}

func (tree *Tree) Set(key []byte, value uint64) {
	keydata := (*C.char)(unsafe.Pointer(&key[0]))
	C.cppbtree_set(tree.ptr, keydata, C.int(len(key)), C.ulonglong(value))
}

func (tree *Tree) Delete(key []byte) {
	keydata := (*C.char)(unsafe.Pointer(&key[0]))
	C.cppbtree_erase(tree.ptr, keydata, C.int(len(key)))
}

func (tree *Tree) Get(key []byte) (uint64, bool) {
	keydata := (*C.char)(unsafe.Pointer(&key[0]))
	var ok C.int
	value := C.cppbtree_get(tree.ptr, keydata, C.int(len(key)), &ok)
	return uint64(value), ok!=0
}

func (tree *Tree) Seek(key []byte) (*Enumerator, error) {
	keydata := (*C.char)(unsafe.Pointer(&key[0]))
	e := &Enumerator{
		tree:  tree.ptr,
		iter: C.cppbtree_seek(tree.ptr, keydata, C.int(len(key))),
	}
	return e, nil
}

func (tree *Tree) SeekFirst(key []byte) (*Enumerator, error) {
	e := &Enumerator{
		tree:  tree.ptr,
		iter: C.cppbtree_seekfirst(tree.ptr),
	}
	return e, nil
}

func (e *Enumerator) Close() {
	C.iter_delete(e.iter)
}

func (e *Enumerator) Next() (k []byte, v uint64, err error) {
	res := C.iter_next(e.tree, e.iter)
	v = uint64(res.value)
	err = nil
	if res.is_valid == 0 {
		err = io.EOF
	}
	k = C.GoBytes(res.key, res.key_len)
	return
}

func (e *Enumerator) Prev() (k []byte, v uint64, err error) {
	res := C.iter_prev(e.tree, e.iter)
	v = uint64(res.value)
	err = nil
	if res.is_valid == 0 {
		err = io.EOF
	}
	k = C.GoBytes(res.key, res.key_len)
	return
}


