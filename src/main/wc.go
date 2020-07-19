package main

import "os"
import "fmt"
import "mapreduce"
import "container/list"

import "regexp"
import "strconv"

// our simplified version of MapReduce does not supply a
// key to the Map function, as in the paper; only a value,
// which is a part of the input file contents
func Map(value string) *list.List {
	// Note: The value argument holds one line of text from the file.
	// You need to:
	// (1) Split up the string into words, discarding any punctuation
	// (2) Add each word to the list with a mapreduce.KeyValue struct
    non_alpha_numeric_regex := regexp.MustCompile(`[^a-zA-Z0-9]+`)
    words_array := non_alpha_numeric_regex.Split(value, -1)
    word_count_map := map[string]int {}
    for i := 0; i < len(words_array); i++ {
        if current_word_count, ok := word_count_map[words_array[i]]; ok {
            word_count_map[words_array[i]] = current_word_count + 1
        } else {
            word_count_map[words_array[i]] = 1
        }
    }

    result := list.New()

    for word, word_count := range word_count_map {
        result.PushBack(mapreduce.KeyValue { word, strconv.Itoa(word_count) })
    }

    return result
}

// iterate over list and add values
func Reduce(key string, values *list.List) string {
	// Note:
	// The key argument holds the key common to all values in the values argument
	// The values argument is a list of value strings with the given key.
	// You need to:
	// (1) Reduce the all of the values in the values list
	// (2) Return the reduced/summed up values as a string
   sum := 0
   for count_str_it := values.Front(); count_str_it != nil; count_str_it = count_str_it.Next() {
       count_str := count_str_it.Value.(string)
       count, _ := strconv.Atoi(count_str)
       sum += count
   }
   return strconv.Itoa(sum)
}

// Can be run in 3 ways:
// 1) Sequential (e.g., go run wc.go master x.txt sequential)
// 2) Master (e.g., go run wc.go master x.txt localhost:7777)
// 3) Worker (e.g., go run wc.go worker localhost:7777 localhost:7778 &)
func main() {
	if len(os.Args) != 4 {
		fmt.Printf("%s: see usage comments in file\n", os.Args[0])
	} else if os.Args[1] == "master" {
		if os.Args[3] == "sequential" {
			mapreduce.RunSingle(5, 3, os.Args[2], Map, Reduce)
		} else {
			mr := mapreduce.MakeMapReduce(5, 3, os.Args[2], os.Args[3])
			// Wait until MR is done
			<-mr.DoneChannel
		}
	} else {
		mapreduce.RunWorker(os.Args[2], os.Args[3], Map, Reduce, 100)
	}
}
