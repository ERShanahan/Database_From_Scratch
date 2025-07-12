#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>
#include <stdbool.h>

#include "table.h"
#include "node.h"

uint32_t* leaf_node_num_cells(void* node) {
    return node + LEAF_NODE_NUM_CELLS_OFFSET;
}

void* leaf_node_cell(void* node, uint32_t cell_num) {
    return node + LEAF_NODE_HEADER_SIZE + cell_num * LEAF_NODE_CELL_SIZE;
}

uint32_t* leaf_node_next_leaf(void* node) {
	return node + LEAF_NODE_NEXT_LEAF_OFFSET;
}

uint32_t* leaf_node_key(void* node, uint32_t cell_num) {
    return leaf_node_cell(node, cell_num);
}

void* leaf_node_value(void* node, uint32_t cell_num) {
    return leaf_node_cell(node, cell_num) + LEAF_NODE_KEY_SIZE;
}

uint32_t* internal_node_num_keys(void* node) {
	return node + INTERNAL_NODE_NUM_KEYS_OFFSET;
}

uint32_t* internal_node_right_child(void* node) {
	return node + INTERNAL_NODE_RIGHT_CHILD_OFFSET;
}

uint32_t* internal_node_cell(void* node, uint32_t cell_num) {
	return node + INTERNAL_NODE_HEADER_SIZE + cell_num * INTERNAL_NODE_CELL_SIZE;
}

uint32_t* internal_node_child(void* node, uint32_t child_num) {
	uint32_t num_keys = *internal_node_num_keys(node);
	if (child_num > num_keys) {
		printf("Tried to access child_num %d > num_keys %d\n", child_num, num_keys);
		exit(EXIT_FAILURE);
	} else if (child_num == num_keys) {
		uint32_t* right_child = internal_node_right_child(node);
		if (*right_child == INVALID_PAGE_NUM) {
			printf("Tried to access right cihld of node, but was invalid page\n");
			exit(EXIT_FAILURE);
		}
		return right_child;
	} else {
		uint32_t* child = internal_node_cell(node, child_num);
		if (*child == INVALID_PAGE_NUM) {
			printf("Tried to access child %d of node, but was invalid page\n", child_num);
			exit(EXIT_FAILURE);
		}
		return child;
	}
}

uint32_t* internal_node_key(void* node, uint32_t key_num) {
	return (void*)internal_node_cell(node, key_num) + INTERNAL_NODE_CHILD_SIZE;
}

bool is_node_root(void* node) {
	uint8_t value = *((uint8_t*)(node + IS_ROOT_OFFSET));
	return (bool)value;
}

void set_node_root(void* node, bool is_root) {
	uint8_t value = is_root;
	*((uint8_t*)(node + IS_ROOT_OFFSET)) = value;
}

NodeType get_node_type(void* node) {
	uint8_t value = *((uint8_t*)(node + NODE_TYPE_OFFSET));
	return (NodeType)value;
}

void set_node_type(void* node, NodeType type) {
	uint8_t value = type;
	*((uint8_t*)(node + NODE_TYPE_OFFSET)) = value;
}

uint32_t get_node_max_key(Pager* pager, void* node) {
	if (get_node_type(node) == NODE_LEAF) {
		return *leaf_node_key(node, *leaf_node_num_cells(node) - 1);
	}
	void* right_child = get_page(pager, *internal_node_right_child(node));
	return get_node_max_key(pager, right_child);
}

uint32_t* node_parent(void* node) { return node + PARENT_POINTER_OFFSET; }

uint32_t internal_node_find_child(void* node, uint32_t key) {

	uint32_t num_keys = *internal_node_num_keys(node);

	uint32_t min_index = 0;
	uint32_t max_index = num_keys;

	while (min_index != max_index) {
		uint32_t index = (min_index + max_index) / 2;
		uint32_t key_to_right = *internal_node_key(node, index);
		if (key_to_right >= key) {
			max_index = index;
		} else {
			min_index = index + 1;
		}
	}

	return min_index;
}

void internal_node_insert(Table* table, uint32_t parent_page_num, uint32_t child_page_num) {
	void* parent = get_page(table->pager, parent_page_num);
	void* child = get_page(table->pager, child_page_num);
	uint32_t child_max_key = get_node_max_key(table->pager, child);
	uint32_t index = internal_node_find_child(parent, child_max_key);

	uint32_t original_num_keys = *internal_node_num_keys(parent);

	if (original_num_keys >= INTERNAL_NODE_MAX_CELLS) {
		internal_node_split_and_insert(table, parent_page_num, child_page_num);
		return;
	}

	uint32_t right_child_page_num = *internal_node_right_child(parent);
	if (right_child_page_num == INVALID_PAGE_NUM) {
		*internal_node_right_child(parent) = child_page_num;
		return;
	}
	void* right_child = get_page(table->pager, right_child_page_num);
	*internal_node_num_keys(parent) = original_num_keys + 1;

	if (child_max_key > get_node_max_key(table->pager, right_child)) {
		*internal_node_child(parent, original_num_keys) = right_child_page_num;
		*internal_node_key(parent, original_num_keys) = get_node_max_key(table->pager, right_child);
		*internal_node_right_child(parent) = child_page_num;
	} else {
		for (uint32_t i = original_num_keys; i > index; i--) {
			void* destination = internal_node_cell(parent, i);
			void* source = internal_node_cell(parent, i - 1);
			memcpy(destination, source, INTERNAL_NODE_CELL_SIZE);
		}
		*internal_node_child(parent, index) = child_page_num;
		*internal_node_key(parent, index) = child_max_key;
	}
}

void internal_node_split_and_insert(Table* table, uint32_t parent_page_num, uint32_t child_page_num) {

	uint32_t old_page_num = parent_page_num;
	void* old_node = get_page(table->pager, parent_page_num);
	uint32_t old_max = get_node_max_key(table->pager, old_node);

	void* child = get_page(table->pager, child_page_num);
	uint32_t child_max = get_node_max_key(table->pager, child);

	uint32_t new_page_num = get_unused_page_num(table->pager);

	uint32_t splitting_root = is_node_root(old_node);

	void* parent;
	void* new_node;
	if (splitting_root) {
		create_new_root(table, new_page_num);
		parent = get_page(table->pager, table->root_page_num);

		old_page_num = *internal_node_child(parent, 0);
		old_node = get_page(table->pager, old_page_num);
	} else {
		parent = get_page(table->pager, *node_parent(old_node));
		new_node = get_page(table->pager, new_page_num);
		initialize_internal_node(new_node);
	}

	uint32_t* old_num_keys = internal_node_num_keys(old_node);

	uint32_t cur_page_num = *internal_node_right_child(old_node);
	void* cur = get_page(table->pager, cur_page_num);

	internal_node_insert(table, new_page_num, cur_page_num);
	*node_parent(cur) = new_page_num;
	*internal_node_right_child(old_node) = INVALID_PAGE_NUM;

	for (int i = INTERNAL_NODE_MAX_CELLS - 1; i > INTERNAL_NODE_MAX_CELLS / 2; i--) {
		cur_page_num = *internal_node_child(old_node, i);
		cur = get_page(table->pager, cur_page_num);

		internal_node_insert(table, new_page_num, cur_page_num);
		*node_parent(cur) = new_page_num;

		(*old_num_keys)--;
	}

	*internal_node_right_child(old_node) = *internal_node_child(old_node, *old_num_keys - 1);
	(*old_num_keys)--;

	uint32_t max_after_split = get_node_max_key(table->pager, old_node);

	uint32_t destination_page_num = child_max < max_after_split ? old_page_num : new_page_num;

	internal_node_insert(table, destination_page_num, child_page_num);
	*node_parent(child) = destination_page_num;

	update_internal_node_key(parent, old_max, get_node_max_key(table->pager, old_node));

	if (!splitting_root) {
		internal_node_insert(table, *node_parent(old_node), new_page_num);
		*node_parent(new_node) = *node_parent(old_node);
	}

}

void update_internal_node_key(void* node, uint32_t old_key, uint32_t new_key){
	uint32_t old_child_index = internal_node_find_child(node, old_key);
	*internal_node_key(node, old_child_index) = new_key;
}

void initialize_leaf_node(void* node) {
	set_node_type(node, NODE_LEAF);
	set_node_root(node, false);
    *leaf_node_num_cells(node) = 0;
	*leaf_node_next_leaf(node) = 0;
}

void initialize_internal_node(void* node) {
	set_node_type(node, NODE_INTERNAL);
	set_node_root(node, false);
	*internal_node_num_keys(node) = 0;
	*internal_node_right_child(node) = INVALID_PAGE_NUM;
}

void create_new_root(Table* table, uint32_t right_child_page_num) {
	void* root = get_page(table->pager, table->root_page_num);
	void* right_child = get_page(table->pager, right_child_page_num);
	uint32_t left_child_page_num = get_unused_page_num(table->pager);
	void* left_child = get_page(table->pager, left_child_page_num);

	memcpy(left_child, root, PAGE_SIZE);
	set_node_root(left_child, false);

	initialize_internal_node(root);
	set_node_root(root, true);
	*internal_node_num_keys(root) = 1;
	*internal_node_child(root, 0) = left_child_page_num;
	uint32_t left_child_max_key = get_node_max_key(table->pager, left_child);
	*internal_node_key(root, 0) = left_child_max_key;
	*internal_node_right_child(root) = right_child_page_num;
	*node_parent(left_child) = table->root_page_num;
	*node_parent(right_child) = table->root_page_num;
}

void leaf_node_split_and_insert(Cursor* cursor, uint32_t key, Row* value) {
	void* old_node = get_page(cursor->table->pager, cursor->page_num);
	uint32_t old_max = get_node_max_key(cursor->table->pager, old_node);
	uint32_t new_page_num = get_unused_page_num(cursor->table->pager);
	void* new_node = get_page(cursor->table->pager, new_page_num);
	initialize_leaf_node(new_node);
	*node_parent(new_node) = *node_parent(old_node);
	*leaf_node_next_leaf(new_node) = *leaf_node_next_leaf(old_node);
	*leaf_node_next_leaf(old_node) = new_page_num;

	for (int32_t i = LEAF_NODE_MAX_CELLS; i >= 0; i--) {
		void* destination_node;
		if (i >= LEAF_NODE_LEFT_SPLIT_COUNT) {
			destination_node = new_node;
		} else {
			destination_node = old_node;
		}
		uint32_t index_within_node = i % LEAF_NODE_LEFT_SPLIT_COUNT;
		void* destination = leaf_node_cell(destination_node, index_within_node);

		if (i == cursor->cell_num) {
			serialize_row(value, destination);
			serialize_row(value, leaf_node_value(destination_node, index_within_node));
			*leaf_node_key(destination_node, index_within_node) = key;
		} else if (i > cursor->cell_num) {
			memcpy(destination, leaf_node_cell(old_node, i - 1), LEAF_NODE_CELL_SIZE);
		} else {
			memcpy(destination, leaf_node_cell(old_node, i), LEAF_NODE_CELL_SIZE);
		}
	}

	*(leaf_node_num_cells(old_node)) = LEAF_NODE_LEFT_SPLIT_COUNT;
	*(leaf_node_num_cells(new_node)) = LEAF_NODE_RIGHT_SPLIT_COUNT;

	if (is_node_root(old_node)) {
		return create_new_root(cursor->table, new_page_num);
	} else {
		uint32_t parent_page_num = *node_parent(old_node);
		uint32_t new_max = get_node_max_key(cursor->table->pager, old_node);
		void* parent = get_page(cursor->table->pager, parent_page_num);

		update_internal_node_key(parent, old_max, new_max);
		internal_node_insert(cursor->table, parent_page_num, new_page_num);
		return;
	}
}

void leaf_node_insert(Cursor* cursor, uint32_t key, Row* value) {
	void* node = get_page(cursor->table->pager, cursor->page_num);
	
	uint32_t num_cells = *leaf_node_num_cells(node);
	if (num_cells >= LEAF_NODE_MAX_CELLS) {
		leaf_node_split_and_insert(cursor, key, value);
		return;
	}

	if (cursor->cell_num < num_cells) {
		for (uint32_t i = num_cells; i > cursor->cell_num; i--) {
			memcpy(leaf_node_cell(node, i), leaf_node_cell(node, i - 1), LEAF_NODE_CELL_SIZE);
		}
	}

	*(leaf_node_num_cells(node)) += 1;
	*(leaf_node_key(node, cursor->cell_num)) = key;
	serialize_row(value, leaf_node_value(node, cursor->cell_num));
}