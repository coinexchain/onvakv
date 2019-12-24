#ifndef CPPBTREE_H
#define CPPBTREE_H

#ifdef __cplusplus
extern "C" {
#endif

	typedef struct {
		void* key;
		int key_len;
		unsigned long long value;
		int is_valid;
	} KVPair;

	void* cppbtree_new();
	void  cppbtree_delete(void* tree);

	unsigned long long cppbtree_put_new_and_get_old(void* tree, char* key, int key_len, unsigned long long value, int *ok);

	void  cppbtree_set(void* tree, char* key, int key_len, unsigned long long value);
	void  cppbtree_erase(void* tree, char* key, int key_len);
	unsigned long long cppbtree_get(void* tree, char* key, int key_len, int* ok);

	void* cppbtree_seek(void* tree, char* key, int key_len);
	void* cppbtree_seekfirst(void* tree);

	KVPair iter_prev(void* tree, void* ptr);
	KVPair iter_next(void* tree, void* ptr);
	void   iter_delete(void* iter);

#ifdef __cplusplus
}
#endif

#endif
