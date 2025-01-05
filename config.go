package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/iancoleman/strcase"
	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/env"
	"github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/providers/posflag"
	"github.com/knadh/koanf/v2"
	"github.com/spf13/pflag"
)

type DhtConf struct {
	VnodeNum           int `koanf:"vnode_num"`
	SuccListLength     int `koanf:"succlist_length"`
	StabilizeInterval  int `koanf:"stabilize_interval"`
	FixFingersInterval int `koanf:"fixfingers_interval"`
}

type DistributedConf struct {
	Addr              string  `koanf:"addr"`
	Bootstrap         string  `koanf:"bootstrapNode"`
	BatchPeriodMs     int     `koanf:"batchPeriod"`
	CheckKeysPeriodMs int     `koanf:"checkKeysPeriod"`
	Dht               DhtConf `koanf:"dht"`
}

type FrontierConf struct {
	MaxActiveQueues      int    `koanf:"maxActive"`
	Politeness           int    `koanf:"politeness"`
	DefaultSessionBudget int    `koanf:"sessionBudget"`
	Seed                 string `koanf:"seed"`
}

type FetcherConf struct {
	Indexer   string `koanf:"indexer"`
	TimeoutMs int    `koanf:"timeout"`
}

type Config struct {
	Distributed DistributedConf `koanf:"distributed"`
	Frontier    FrontierConf    `koanf:"frontier"`
	Fetcher     FetcherConf     `koanf:"fetcher"`
}

func ReadConf() (*Config, error) {
	k := koanf.New(".")

	flags := ParseFlags()

	yamlPath, _ := flags.GetString("conf")
	println(yamlPath)
	k.Load(file.Provider(yamlPath), yaml.Parser()) //read yaml first

	k.Load(env.Provider("", ".", func(s string) string { //overwrite with .env if exists
		replaced := strings.Replace(strings.ToLower(s), "__", "#", -1)
		replaced = strings.Replace(replaced, "_", ".", -1)
		return strcase.ToSnakeWithIgnore(strings.Replace(replaced, "#", " ", -1), ".") //ugh...
	}), nil)

	k.Load(posflag.Provider(flags, ".", k), nil)
	var conf Config
	k.Unmarshal("", &conf)
	return &conf, nil
}

func ParseFlags() *pflag.FlagSet {
	f := pflag.NewFlagSet("config", pflag.ContinueOnError)
	f.Usage = func() {
		fmt.Println(f.FlagUsages())
		os.Exit(0)
	}

	f.String("conf", "config.yaml", "configuration file")
	f.String("distributed.addr", "", "defines node address")
	f.String("distributed.bootstrap_node", "", "node to bootstrap with")
	f.String("frontier.seed", "", "seed list path")
	f.String("fetcher.indexer", "", "indexer address")

	f.Parse(os.Args[1:])

	return f
}
