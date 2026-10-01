// A bot that keeps its TheDiscordList listing up to date and receives its
// webhooks.
//
//	DISCORD_TOKEN=... TDL_TOKEN=... TDL_WEBHOOK_SECRET=... go run ./examples/bot
package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"os/signal"

	"github.com/bwmarrin/discordgo"

	thediscordlist "github.com/TheDiscordList/integration-discordgo"
	"github.com/TheDiscordList/integration-discordgo/webhooks"
)

func main() {
	session, err := discordgo.New("Bot " + os.Getenv("DISCORD_TOKEN"))
	if err != nil {
		log.Fatal(err)
	}
	session.Identify.Intents = discordgo.IntentsGuilds

	poster := thediscordlist.Autopost(session, os.Getenv("TDL_TOKEN"),
		thediscordlist.OnPosted(func(kind thediscordlist.Kind, status int) { log.Printf("tdl: %s -> %d", kind, status) }),
		thediscordlist.OnError(func(err *thediscordlist.Error) { log.Printf("tdl: %v", err) }),
		thediscordlist.OnDisabled(func(kind thediscordlist.Kind, status int, reason string) {
			log.Printf("tdl: %s disabled (%d): %s", kind, status, reason)
		}),
	)

	if secret := os.Getenv("TDL_WEBHOOK_SECRET"); secret != "" {
		http.Handle("/webhooks/tdl", webhooks.NewHandler(secret, func(ctx context.Context, event *webhooks.Event, d webhooks.Delivery) error {
			if event.Event == webhooks.EventListingUpvoted {
				var vote webhooks.Upvoted
				if err := json.Unmarshal(event.Data, &vote); err == nil && vote.User != nil {
					log.Printf("%s upvoted %s (%d total)", vote.User.Username, event.Listing.Name, vote.TotalUpvotes)
				}
			}
			return nil
		}))
		go func() { log.Println(http.ListenAndServe(":8080", nil)) }()
	}

	if err := session.Open(); err != nil {
		log.Fatal(err)
	}
	defer session.Close()
	defer poster.Stop()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt)
	<-stop
}
