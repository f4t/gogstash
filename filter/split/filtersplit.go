package filteraddfield

import (
	"context"

	"github.com/tsaikd/gogstash/config"
	"github.com/tsaikd/gogstash/config/logevent"
)

// ModuleName is the name used in config file
const ModuleName = "split"

// FilterConfig holds the configuration json fields and internal objects
type FilterConfig struct {
	config.FilterConfig
	Source string `json:"split_source"`
	// Value string `json:"value"`
}

// DefaultFilterConfig returns an FilterConfig struct with default values
func DefaultFilterConfig() FilterConfig {
	return FilterConfig{
		FilterConfig: config.FilterConfig{
			CommonConfig: config.CommonConfig{
				Type: ModuleName,
			},
		},
	}
}

// InitHandler initialize the filter plugin
func InitHandler(ctx context.Context, raw *config.ConfigRaw) (config.TypeFilterConfig, error) {
	conf := DefaultFilterConfig()
	if err := config.ReflectConfig(raw, &conf); err != nil {
		return nil, err
	}

	return &conf, nil
}

func CloneEvent(event logevent.LogEvent) logevent.LogEvent {
	evt := logevent.LogEvent{
		Timestamp: event.Timestamp,
		Tags:      event.Tags,
		Message:   event.Message,
		Extra:     make(map[string]interface{}),
	}

	// Copy from the original map to the target map
	for key, value := range event.Extra {
		evt.Extra[key] = value
	}

	return evt
}

// Event the main filter event
func (f *FilterConfig) Event(ctx context.Context, event logevent.LogEvent) ([]logevent.LogEvent, bool) {

	eventsOut := make([]logevent.LogEvent, 0)
	if _, ok := event.Extra[f.Source]; !ok {
		eventsOut = append(eventsOut, event)
		event.AddTag("gogstash_filter_split_error")
		return eventsOut, false
	}

	splitItems, _ := event.GetValue(f.Source)
	event.Remove(f.Source)

	for _, elem := range splitItems.([]interface{}) {
		evt := CloneEvent(event)
		evt.SetValue(f.Source, elem)
		eventsOut = append(eventsOut, evt)
	}

	return eventsOut, true
}
