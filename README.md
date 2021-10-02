# go-whosonfirst-iterate-sqlite

Go package implementing go-whosonfirst-iterate/emitter functionality for (Who's On First specific) SQLite databases.

## Important

Documentation for this package is incomplete and will be updated shortly.

## Tools

```
> make cli
go build -mod vendor -o bin/count cmd/count/main.go
go build -mod vendor -o bin/emit cmd/emit/main.go
```

### count

```
$> ./bin/count /usr/local/data/sfomuseum-data-flights-2020-latest.db
2021/02/18 10:57:42 time to index paths (1) 1m46.893753865s
2021/02/18 10:57:42 Counted 752289 records (saw 752289 records)
```

### emit
$> ./bin/emit \
	-geojson \
	-emitter-uri 'sqlite://?include=properties.icao:airline=ANZ' \
	/usr/local/data/sfomuseum-data-flights-2020-latest.db

| jq '.features[]["properties"]["wof:name"]'
```
"NZ9198 (SFO-IAH)"
"NZ9147 (BFL-SFO)"
"NZ9130 (EWR-SFO)"
"NZ9225 (SFO-LAS)"
"NZ9525 (SFO-MFR)"
"NZ9340 (SFO-PDX)"
"NZ9716 (SNA-SFO)"
"NZ9124 (EWR-SFO)"
"NZ9131 (MIA-SFO)"
"NZ9282 (LAX-SFO)"
"NZ9353 (PDX-SFO)"
"NZ9238 (LAS-SFO)
... and so on
```

## See also

* https://github.com/whosonfirst/go-whosonfirst-iterate
* https://github.com/aaronland/go-sqlite