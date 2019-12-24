#include "./cpp-btree-1.0.1/btree_map.h"
#include "cppbtree.h"

typedef btree::btree_map<std::string, unsigned long long> BTree;
typedef BTree::iterator Iter;

void* cppbtree_new() {
	BTree* bt = new BTree();
	return (void*)bt;
}
void  cppbtree_delete(void* tree) {
	BTree* bt = (BTree*)tree;
	delete bt;
}

unsigned long long cppbtree_put_new_and_get_old(void* tree, char* key, int key_len, unsigned long long value, int *ok) {
	std::string keyStr(key, key_len);
	BTree* bt = (BTree*)tree;
	Iter iter = bt->find(keyStr); 
	if (iter == bt->end()) {
		(*bt)[keyStr] = value;
		*ok = 0;
		return 0;
	} else {
		unsigned long long old_value = iter->second;
		iter->second = value;
		*ok = 1;
		return old_value;
	}
}

void  cppbtree_set(void* tree, char* key, int key_len, unsigned long long value) {
	std::string keyStr(key, key_len);
	BTree* bt = (BTree*)tree;
	(*bt)[keyStr] = value;
}
void  cppbtree_erase(void* tree, char* key, int key_len) {
	std::string keyStr(key, key_len);
	BTree* bt = (BTree*)tree;
	bt->erase(keyStr);
}
unsigned long long cppbtree_get(void* tree, char* key, int key_len, int* ok) {
	std::string keyStr(key, key_len);
	BTree* bt = (BTree*)tree;
	Iter iter = bt->find(keyStr);
	if(iter == bt->end()) {
		*ok = 0;
		return 0;
	} else {
		*ok = 1;
		return iter->second;
	}
}

void* cppbtree_seek(void* tree, char* key, int key_len) {
	Iter* iter = new Iter();
	std::string keyStr(key, key_len);
	BTree* bt = (BTree*)tree;
	*iter = bt->find(keyStr);
	return (void*)iter;
}
void* cppbtree_seekfirst(void* tree) {
	Iter* iter = new Iter();
	BTree* bt = (BTree*)tree;
	*iter = bt->begin();
	return (void*)iter;
}

KVPair iter_next(void* tree, void* ptr) {
	KVPair res;
	BTree* bt = (BTree*)tree;
	Iter* iter = (Iter*)ptr;
	std::pair<std::string, unsigned long long> kv = *(*iter);
	res.key = (void*)kv.first.data();
	res.value = kv.second;
	res.key_len = kv.first.size();
	res.is_valid = (bt->end()==*iter)? 0 : 1;
	iter->increment();
	return res;
}
KVPair iter_prev(void* tree, void* ptr) {
	KVPair res;
	BTree* bt = (BTree*)tree;
	Iter* iter = (Iter*)ptr;
	std::pair<std::string, unsigned long long> kv = *(*iter);
	res.key = (void*)kv.first.data();
	res.value = kv.second;
	res.key_len = kv.first.size();
	res.is_valid = (bt->end()==*iter)? 0 : 1;
	iter->decrement();
	return res;
}
void  iter_delete(void* ptr) {
	Iter* iter = (Iter*)ptr;
	delete iter;
}
