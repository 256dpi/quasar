module github.com/256dpi/quasar

go 1.12

require (
	github.com/256dpi/turing v0.0.0-20200413151708-a1a9d66c9d69
	github.com/cockroachdb/pebble v0.0.0-20200403212304-5761da73f80a
	github.com/golang/snappy v0.0.1 // indirect
	github.com/kr/pretty v0.1.0
	github.com/niemeyer/pretty v0.0.0-20200227124842-a10e7caefd8e // indirect
	github.com/stretchr/testify v1.4.0
	golang.org/x/net v0.0.0-20190613194153-d28f0bde5980 // indirect
	golang.org/x/sys v0.0.0-20200122134326-e047566fdf82 // indirect
	gopkg.in/check.v1 v1.0.0-20200227125254-8fa46927fb4f // indirect
	gopkg.in/tomb.v2 v2.0.0-20161208151619-d5d1b5820637
	gopkg.in/yaml.v2 v2.2.5 // indirect
)

replace github.com/cockroachdb/pebble v0.0.0-20200403212304-5761da73f80a => github.com/256dpi/pebble v0.0.0-20200412075638-40cf26bb0e73
