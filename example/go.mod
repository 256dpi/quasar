module example

go 1.12

require (
	github.com/256dpi/god v0.4.2
	github.com/256dpi/quasar v0.0.0
	github.com/montanaflynn/stats v0.5.0
)

replace github.com/256dpi/quasar => ../
replace github.com/cockroachdb/pebble v0.0.0-20200403212304-5761da73f80a => github.com/256dpi/pebble v0.0.0-20200412075638-40cf26bb0e73
