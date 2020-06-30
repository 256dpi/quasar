module github.com/256dpi/quasar

go 1.14

require (
	github.com/256dpi/turing v0.0.0-20200630074246-efd36e7f189f
	github.com/stretchr/testify v1.4.0
	gopkg.in/tomb.v2 v2.0.0-20161208151619-d5d1b5820637
)

replace github.com/cockroachdb/pebble v0.0.0-20200219202912-046831eaec09 => github.com/256dpi/pebble v0.0.0-20200414073916-7b64097a81ce
