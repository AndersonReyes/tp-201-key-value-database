package main

import (
	"fmt"
	"log"
	"os"

	"github.com/urfave/cli/v2"
)

func main() {
	app := &cli.App{
		Name:  "kv-store",
		Usage: "persistent key valye store",
		Commands: []*cli.Command{
			{
				Name:  "get",
				Usage: "get key",
				Action: func(cCtx *cli.Context) error {
					key := cCtx.Args().First()
					fmt.Println("unimplemented", key)
					return nil
				},
			},
			{
				Name:  "rm",
				Usage: "remove key",
				Action: func(cCtx *cli.Context) error {
					key := cCtx.Args().First()
					fmt.Println("unimplemented", key)
					return nil
				},
			},
			{
				Name:  "set ",
				Usage: "set key with value",
				Action: func(cCtx *cli.Context) error {
					key := cCtx.Args().First()
					value := cCtx.Args().Get(1)
					fmt.Println("unimplemented", key, value)
					return nil
				},
			},
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}
