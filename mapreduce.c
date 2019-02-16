#include "mapreduce.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>

/*bst for the partitions*/
typedef struct bst_node bst_node;
typedef struct v_node v_node;

/*Values for a particular key will be stored in a link list*/
struct v_node{
  char* value;
  v_node* next;
};

/*Each partition will consist of a binary search tree where
  each node corresponds to a key.*/
struct bst_node{
  char* key;
  v_node* v_head; //linked list of values
  bst_node* left;
  bst_node* right;
  bst_node* parent;
};
bst_node** low_key;  //stores the current lowest key value
bst_node** bst_heads; //bst heads of the partitions
int num_partitions;
pthread_mutex_t* bst_locks;

/*Used for mapping*/
Mapper mapper;
void* map_thread(void* args);
Partitioner parter;
volatile int current_file;
char** files;
pthread_mutex_t files_lock = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t  get_file = PTHREAD_COND_INITIALIZER;
void add_to_tree(int part_num, char* key, char* value);
void value_push(bst_node* node, char* value);

/*Used for reducing*/
Reducer reducer;          
void* reduce_thread(void* args); 
char* get_next_value(char *key, int partition_number);
char* get_low_key(int partition_num);
void free_node(bst_node* node);
v_node** cur_values; 
int* new_key;       
                    

/*Adds the intermediate key, value pairs
  produced by the user map function into 
  the bst of the correct partition.*/
void
MR_Emit(char *key, char *value)
{
  /*Check for valid arguments.*/
  if(key == NULL || value == NULL){
    printf("invalid args to MR_Emit\n");
    exit(1);
  }

  /*Determine the partition number for this key.*/
  int part = parter(key, num_partitions);
  
  /*Grab the appropriate partition lock and
    add the value to the bst*/
  pthread_mutex_lock(&bst_locks[part]);
  add_to_tree(part, key, value);
  pthread_mutex_unlock(&bst_locks[part]);
  return;
}

/*The "master" function. Creates the threads, the
  global data structures to be used, and waits for
  all the threads to do their work before returning.*/
void
MR_Run(int argc, char* argv[],
       Mapper map, int num_mappers,
       Reducer reduce, int num_reducers,
       Partitioner partition)
{
  pthread_t threads[num_reducers+num_mappers];
  num_partitions = num_reducers;
  int partition_nums[num_partitions];
  current_file = 0;
 
  /*Check for valid arguments.*/
  if(argc <= 1 || map == NULL || num_mappers <= 0 ||
     reduce == NULL || num_reducers <= 0) {
    printf("invalid arguments\n");
    exit(1);
  }

  /*Set file info*/
  files = malloc(sizeof(char*)*argc);
  for(int i = 0; i < argc - 1; i++)
    files[i] = argv[i + 1];
  files[argc - 1] = NULL;
  
  /*Set the function values*/
  mapper = map;
  reducer = reduce;
  if(partition == NULL)
    parter = MR_DefaultHashPartition;
  else
    parter = partition;

  /*Initialize the bst_locks*/
  bst_locks = malloc(sizeof(pthread_mutex_t)*num_reducers);
  for(int i = 0; i < num_reducers; i++)
    pthread_mutex_init(&bst_locks[i], NULL);

  /*Initialize the bst and other values used for reducing*/
  bst_heads = malloc(sizeof(bst_node*)*num_reducers);
  low_key   = malloc(sizeof(bst_node*)*num_reducers);
  cur_values = malloc(sizeof(v_node*)*num_reducers);
  new_key    = malloc(sizeof(int)*num_reducers);
  for(int i = 0; i < num_reducers; i++) {
    bst_heads[i] = NULL;
    low_key[i]  = NULL;
    cur_values[i] = NULL;
    new_key[i] = 1;
  }
  
  /*Create the mapper threads and wait for them to finish*/
  for(int i = 0; i < num_mappers; i++)
    pthread_create(&(threads[i]), NULL, map_thread, NULL);
  for(int i = 0; i < num_mappers; i++)
    pthread_join(threads[i], NULL);
  free(files);

  /*Don't need the locks anymore.*/
  for(int i = 0; i < num_reducers; i++)
    pthread_mutex_destroy(&bst_locks[i]);
  free(bst_locks);
  
  /*Create and wait for the reducer functions. Give each thread
    a unique partition number*/
  for(int i = 0; i < num_reducers; i++){
    partition_nums[i] = i;
    pthread_create(&(threads[i]), NULL, reduce_thread, &partition_nums[i]);
  }
  for(int i = 0; i < num_reducers; i++)
    pthread_join(threads[i], NULL);

  /*Free other allocated memory and return*/
  free(cur_values);
  free(new_key);
  free(low_key);
  free(bst_heads);
  return;
}

/*Mapper thread. Simply gets the next file to be processed
  and passes it into the user defined map function.*/
void*
map_thread(void* args)
{
  char* file; //next file to work on
  while(1) {
    pthread_mutex_lock(&files_lock);
    if((file = files[current_file]) == NULL) {
      pthread_mutex_unlock(&files_lock);
      return NULL;
    }
    current_file++;
    pthread_mutex_unlock(&files_lock);
    mapper(file);
  }
}

/*Reducer thread. Simply gets the next low key value and 
  calls the user defined reducer function on it with our
  get_next_value function passed in.*/
void*
reduce_thread(void* args)
{
  int local_part = *(int*)args; //passed in partition#
  char* next_key;
  while(1) {
    /*Each reducer has been given a unique partition
      number, so locks aren't needed. Give it the lowest
      current key in the partition.*/
    if((next_key = get_low_key(local_part)) == NULL)
      return NULL;
    reducer(next_key, get_next_value, local_part);
  }
}

/*Partioner that will be used if the user doesn't specify one*/
unsigned long
MR_DefaultHashPartition(char *key, int num_partitions) {
  unsigned long hash = 5381;
  int c;
  while ((c = *key++) != '\0')
    hash = hash * 33 + c;
  return hash % num_partitions;
}

/*By the design of this program, the key should always be
  equal to low_key[partition_num], so the key value passed
  in is useless.*/
char*
get_next_value(char *key, int partition_num)
{
  char* return_value;
  
  /*We have a new key node to work on.*/
  if(new_key[partition_num] == 1) {
    new_key[partition_num] = 0;
    cur_values[partition_num] = low_key[partition_num]->v_head;
  }

  /*We're done with this key's value list.*/
  if(cur_values[partition_num] == NULL) {
    new_key[partition_num] = 1;
    return NULL;
  }

  /*Grab the next value from the list.*/
  else {
    return_value = cur_values[partition_num]->value;
    cur_values[partition_num] = cur_values[partition_num]->next;
  }

  return return_value;
}

/*Returns the lowest key left for a particular partition, 
  and frees the memory of the last low key that the reducer
  function is now done with. Returns NULL if the partition
  is empty.*/
char*
get_low_key(int partition_num)
{
  //char* return_key;
  bst_node* delete_node; //Node that will be deleted by this function
  bst_node* temp_node;   //for tree adjustment

  /*If the low_key is NULL, this is the first call to this
    function for this partition.*/
  if(low_key[partition_num] == NULL) {

    /*partition is empty at the start.*/
    if(bst_heads[partition_num] == NULL)
      return NULL;

    /*Find the lowest key in the partition.*/
    else {
      low_key[partition_num] = bst_heads[partition_num];

      /*Loop all the way left from the head.*/
      while(low_key[partition_num]->left != NULL)
	low_key[partition_num] = low_key[partition_num]->left;

      return low_key[partition_num]->key; //return the lowest key
    }
  }

  /*Otherwise, we have to find the new low key, adjust the tree(two cases)
    and free the old low node.*/
  else {
    delete_node = low_key[partition_num]; //last low key, will be deleted
    
    /*****************Find the new low node. Three cases:***************/
    /*Case1: last node in the tree*/
    if(delete_node->right == NULL && delete_node->parent == NULL) {
      bst_heads[partition_num] = NULL;
      free_node(delete_node);
      return NULL;
    }
    
    /*Case2: low_key has a parent and no children*/
    else if(delete_node->right == NULL && delete_node->parent != NULL) {

      /*The lowest value is now the parent of the previous low*/
      low_key[partition_num] = low_key[partition_num]->parent;

      /*Adjust the parent.*/
      low_key[partition_num]->left = NULL; //must be the left child.
      free_node(delete_node);
    }
    
    /*Case3: loop to find the lowest node and adjust the tree*/
    else {

      /*This will either be the new head or the new child of the
        previous low's parent.*/
      temp_node = low_key[partition_num]->right;

      /*Previous low was the head of the tree.*/
      if(delete_node == bst_heads[partition_num]) {
	bst_heads[partition_num] = temp_node;
	temp_node->parent = NULL;
      }

      /*Temp node is now the left child of the last low key's parent*/
      else {
	delete_node->parent->left = temp_node;
	temp_node->parent = delete_node->parent;
      }
      
      /*Find the new low key.*/
      while(temp_node->left != NULL)
	temp_node = temp_node->left;
      low_key[partition_num] = temp_node;
      free_node(delete_node);
    }
  }
  return low_key[partition_num]->key;
}

/*Adds the key, value pair to the bst tree for the partition number.*/
void
add_to_tree(int part_num, char* key, char* value)
{
  bst_node* parent_node = bst_heads[part_num];
  bst_node* new_node;
  int cmp;
  
  /*Tree is empty. Set the new node as the head.*/
  if(parent_node == NULL) {
    new_node = malloc(sizeof(bst_node));
    new_node->v_head = NULL;
    new_node->left = NULL;
    new_node->right = NULL;
    new_node->parent = NULL;
    new_node->key = strdup(key);
    bst_heads[part_num] = new_node;
    value_push(new_node, value);
    return;
  }

  while(1) {
    cmp = strcmp(key, parent_node->key);
    /*we already have a node for the key, merge in the value.*/
    if(cmp == 0) {
      value_push(parent_node, value);
      return;
    }

    /*Key value is lower than the current search node.*/
    else if(cmp < 0) {
      /*Found the new parent of the added node*/
      if(parent_node->left == NULL) {
	/*Add the new node as the left child of the parent node.*/
	new_node = malloc(sizeof(bst_node));
	new_node->v_head = NULL;
	new_node->left = NULL;
	new_node->right = NULL;
	parent_node->left = new_node;
	new_node->key = strdup(key);
	new_node->parent = parent_node;
	value_push(new_node, value);
	return;
      }
      /*Keep searching*/
      else {
	parent_node = parent_node->left;
	continue;
      }
    }
    /*Key value is higher than the current search node.*/
    else{
      /*Found the new parent of the added node*/
      if(parent_node->right == NULL) {
	/*Add the new node as the right child of the parent node.*/
	new_node = malloc(sizeof(bst_node));
	new_node->v_head = NULL;
	new_node->left = NULL;
	new_node->right = NULL;
	parent_node->right = new_node;
	new_node->key = strdup(key);
	new_node->parent = parent_node;
	value_push(new_node, value);
	return;
      }
      /*Keep searching*/
      else {
	parent_node = parent_node->right;
	continue;
      }
    }
  }
}

/*Push a new value onto the value list for a key node.*/
void
value_push(bst_node* node, char* value)
{
  /*Allocate the memory for the new value.*/
  v_node* new_value = malloc(sizeof(v_node));
  new_value->value = strdup(value);
  
  /*Value list for this key is empty. This is the new head*/
  if(node->v_head == NULL) {
    new_value->next = NULL;
    node->v_head = new_value;
  }

  /*Push the value into the value "stack".*/
  else {
    new_value->next = node->v_head;
    node->v_head = new_value;
  }
  return;
}

/*Frees up all the contents of a node*/
void
free_node(bst_node* node)
{
  v_node* temp_node; //current value to free
  
  /*First we need to free up the list of values*/
  while(node->v_head != NULL) {
    temp_node = node->v_head;
    node->v_head = node->v_head->next;
    free(temp_node->value);
    free(temp_node);
  }
  
  /*Then the node itself*/
  free(node->key);
  free(node);
}
