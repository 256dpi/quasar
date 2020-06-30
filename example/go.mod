module example

go 1.14

require (
	github.com/256dpi/god v0.4.3
	github.com/256dpi/quasar v0.0.0
	github.com/256dpi/turing v0.0.0-20200630074246-efd36e7f189f
	github.com/montanaflynn/stats v0.5.0
)

replace github.com/256dpi/quasar => ../

replace github.com/cockroachdb/pebble v0.0.0-20200219202912-046831eaec09 => github.com/256dpi/pebble v0.0.0-20200414073916-7b64097a81ce
