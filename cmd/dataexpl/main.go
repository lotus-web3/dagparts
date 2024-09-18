package main

import (
	"github.com/urfave/cli/v2"
	"os"

	logging "github.com/ipfs/go-log/v2"
)

var log = logging.Logger("dataexpl")

func main() {
	logging.SetLogLevel("*", "INFO")

	app := &cli.App{
		Name:    "dataexpl",
		Usage:   "Filecoin data explorer",
		Version: "1",
		Commands: []*cli.Command{
			dataexplCmd,
			computeClientMetaCmd,
		},
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "repo",
				EnvVars: []string{"LOTUS_PATH"},
				Hidden:  true,
				Value:   "~/.lotus", // TODO: Consider XDG_DATA_HOME
			},
			&cli.StringFlag{
				Name:  "log-level",
				Value: "info",
			},
		},
		Before: func(cctx *cli.Context) error {
			return logging.SetLogLevel("dataexpl", cctx.String("log-level"))
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Warnf("%s", err.Error())
		os.Exit(1)
		return
	}
}
