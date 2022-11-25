package main

import (
	"fmt"
	"net"
)

func main() {
	classCMask := net.IPv4Mask(0xff, 0xff, 0xff, 0)
	ip := net.IPv4(127, 127, 127, 127)
	mask := ip.Mask(classCMask)
	fmt.Println(mask.String())
}
