package util;

import (
	"fmt"
	"strconv"
	"strings"
)

// this function expects the input to be in the form of 'nid[005560-005567,005571-005581,001234]' or 'nid001234,nid002345'
// or combinations of these two
// This is working correctly: `nodes=expand_nodes('nid00[5560-5567,5571],asd[001234-001235],qqr006789')`
func ExpandNodes(nodes string) []Node {
	split_nodes := strings.Split(nodes, ",")
	prefix := ""
	nodelist := []Node{}
	for _, node := range split_nodes {
		reset_prefix := false
		if strings.Index(node, "[") != -1 {
			helper := strings.Split(node, "[")
			prefix = helper[0]
			node = helper[1]
		}
		if strings.Index(node, "]") != -1 {
			node = node[:len(node)-1]
			reset_prefix = true
		}
		if strings.Index(node, "-") != -1 {
			helper := strings.Split(node, "-")
			start, _ := strconv.Atoi(helper[0])
			end, _ := strconv.Atoi(helper[1])
			for node_nbr := start; node_nbr<=end; node_nbr++ {
				format := fmt.Sprintf("%%s%%0%dd", len(helper[0])) // e.g. format == "%s%06d"
				nodelist = append(nodelist, Node{Nid:fmt.Sprintf(format, prefix, node_nbr)})
			}
		} else {
			nodelist = append(nodelist, Node{Nid: fmt.Sprintf("%s%s", prefix, node)})
		}
		if reset_prefix {
			prefix = ""
		}
	}
    return nodelist
}
