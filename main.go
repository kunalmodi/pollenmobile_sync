package main

// A script to scrape Pollen Mobile data into a Postgres Database. This can be used as a cron script to keep
// an up-to-date copy of the database. To run:
// 	go run main.go
// If you care about certain regional hexes, you can pass in a Level-5 hex, or a bounding box of Hexes (comma separated).
// Here are some example ones of interest:
// NYC:  "852a1393fffffff,852a104bfffffff,852a1057fffffff,852a1063fffffff,852a100bfffffff,852a106ffffffff,852a13c3fffffff,852a107bfffffff,852a102ffffffff,852a1383fffffff,852a103bfffffff,852a1047fffffff,852a139bfffffff,852a106bfffffff,852a1077fffffff,852a12b7fffffff,852a102bfffffff,852a13d7fffffff,852a138bfffffff,852a1043fffffff,852a1397fffffff,852a104ffffffff,852a1003fffffff,852a1067fffffff,852a100ffffffff,852a12a7fffffff,852a1073fffffff,852a101bfffffff,852a13c7fffffff,852a12b3fffffff,852a13d3fffffff"
// San Francisco: "85283457fffffff,852830c7fffffff,85283467fffffff,8528346ffffffff,85283403fffffff,852830d7fffffff,85283477fffffff,8528340bfffffff,8528341bfffffff,85283083fffffff,8528342bfffffff,8528308bfffffff,85283093fffffff,8528343bfffffff,8528309bfffffff,85283443fffffff,85283453fffffff,852836a7fffffff,852830c3fffffff,85283463fffffff,852830cbfffffff,8528346bfffffff,852836b7fffffff,852830d3fffffff,85283473fffffff,852830dbfffffff,85283407fffffff,8528347bfffffff,8528340ffffffff,85283417fffffff,8528308ffffffff,85283447fffffff,8528344ffffffff"
// Here is a more complete example:
//   go run main.go "852a1393fffffff,852a104bfffffff,852a1057fffffff" "85283457fffffff,852830c7fffffff,85283467fffffff"

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	_ "github.com/joho/godotenv/autoload"
	"github.com/lib/pq"
	"github.com/uber/h3-go/v4"
	"go.uber.org/ratelimit"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/logger"
)

func main() {
	db, err := gorm.Open(postgres.Open(os.Getenv("PG_URL")), &gorm.Config{
		Logger: quietLogger(),
	})
	handleErr(err)
	for _, model := range models {
		handleErr(db.AutoMigrate(&model))
	}
	for _, idx := range indexes {
		handleErr(db.Exec(idx).Error)
	}

	handleErr(initGeocodeCache(db))

	handleErr(syncFlowers(db))
	handleErr(syncRewards(db))
	for _, hexGroup := range os.Args[1:] {
		if hexGroup == "" || !isValidHex(hexGroup) {
			panic(fmt.Errorf("invalid argument passed, should be a comma-separated list of H3 hexes"))
		}
		handleErr(syncHexes(db, hexGroup))
	}
}

func syncRewards(db *gorm.DB) error {
	var flowerNames []string
	err :=
		db.
			Table(tableNameFlower).Select("id").
			Find(&flowerNames).Error
	if err != nil {
		return err
	}
	fmt.Printf("Found %v reward candidates\n", len(flowerNames))
	for i, flowerName := range flowerNames {
		if i%100 == 0 {
			fmt.Printf("Reward progress: %d/%d\n", i, len(flowerNames))
		}

		rewardItems, err := getRewards(flowerName)
		if err != nil {
			return err
		}
		rewards := []Reward{}
		for _, r := range rewardItems {
			coverage := []string{}
			switch v := r.Coverage.(type) {
			case []string:
				coverage = v
			}
			rewards = append(rewards, Reward{
				ID:                r.RewardID,
				PCN:               r.PCN,
				PIC:               r.PIC,
				RSERatio:          r.RSERatio,
				Client:            r.Client,
				Coverage:          pq.StringArray(coverage),
				DailyPIC:          r.DailyPIC,
				Date:              r.Date,
				Device:            r.Device,
				DeviceType:        r.DeviceType,
				Reward:            r.Reward,
				Transaction:       r.Transaction,
				TransactionStatus: r.TransactionStatus,
				Wallet:            r.Wallet,
			})
		}
		if err := db.Clauses(upsertClause).CreateInBatches(&rewards, 200).Error; err != nil {
			return err
		}
	}
	return nil
}

func syncFlowers(db *gorm.DB) error {
	flowerItems, err := getAllFlowers()
	if err != nil {
		return err
	}
	fmt.Printf("Found %v flowers\n", len(flowerItems))

	flowers := make([]Flower, len(flowerItems))
	for i, f := range flowerItems {
		beesSeen, err := json.Marshal(f.BeesSeen)
		if err != nil {
			return err
		}
		geo, err := reverseGeocode(f.H3Hex)
		if err != nil {
			return err
		}
		flowers[i] = Flower{
			ID:                f.ID,
			BountyRewards:     f.BountyRewards,
			DisplayName:       f.DisplayName,
			UpdateTime:        f.UpdateTime,
			DailyBeesSeen:     pq.StringArray(f.DailyBeesSeen),
			FirstSeen:         f.FirstSeen,
			HBeesSeen:         pq.StringArray(f.HBeesSeen),
			WalletAddress:     f.WalletAddress,
			CoveredHexes:      pq.StringArray(f.CoveredHexes),
			LastSeen:          f.LastSeen,
			DailyAttaches:     f.DailyAttaches,
			H3Hex:             f.H3Hex,
			Lat:               geo.Lat,
			Lng:               geo.Lng,
			Address:           geo.Address,
			Suburb:            geo.Suburb,
			City:              geo.City,
			State:             geo.State,
			Town:              geo.Town,
			County:            geo.County,
			Active:            f.Active,
			FlowerRewards:     f.FlowerRewards,
			DailyCoveredHexes: pq.StringArray(f.DailyCoveredHexes),
			NFTAddress:        f.NFTAddress,
			Nickname:          f.Nickname,
			FlowerAttaches:    f.FlowerAttaches,
			DailyHBeesSeen:    pq.StringArray(f.DailyHBeesSeen),
			DailyRewards:      f.DailyRewards,
			ImageURL:          f.ImageURL,
			BeesSeen:          string(beesSeen),
		}
	}
	return db.Clauses(upsertClause).CreateInBatches(&flowers, 200).Error
}

func syncHexes(db *gorm.DB, hexLocations string) error {
	hexes, err := getAllHexes(hexLocations)
	if err != nil {
		return err
	}
	fmt.Printf("Found %v hexes\n", len(hexes))

	for i, hex := range hexes {
		if i%100 == 0 {
			fmt.Printf("Hex progress: %v / %v\n", i, len(hexes))
		}

		details, err := getHexDetails(hex.ID)
		if err != nil {
			return err
		}
		geo, err := reverseGeocode(hex.ID)
		if err != nil {
			return err
		}
		err = db.
			Clauses(upsertClause).
			Create(&Hex{
				ID:               hex.ID,
				FlowerCount:      hex.FlowerCount,
				Covered:          hex.Covered,
				Lat:              geo.Lat,
				Lng:              geo.Lng,
				Address:          geo.Address,
				Suburb:           geo.Suburb,
				City:             geo.City,
				State:            geo.State,
				Town:             geo.Town,
				County:           geo.County,
				Attach:           details.Hex.Attach,
				Flowers:          pq.StringArray(details.Hex.Flowers),
				FlowersContained: pq.StringArray(details.Hex.FlowersContained),
				BountyReward:     details.Hex.BountyReward,
				LootBoxReward:    details.Hex.LootBoxReward,
				DailyReward:      details.Hex.DailyReward,
				Bounty:           details.Hex.Bounty,
				BountyTime:       details.Hex.BountyTime,
			}).
			Error
		if err != nil {
			return err
		}
	}
	return nil
}

// Postgres DB Schema
//
type (
	Hex struct {
		ID               string `gorm:"primaryKey"`
		FlowerCount      int
		Covered          int
		Lat              float64
		Lng              float64
		Address          string
		Suburb           string
		City             string
		State            string
		Town             string
		County           string
		Attach           int
		Flowers          pq.StringArray `gorm:"type:text[]"`
		FlowersContained pq.StringArray `gorm:"type:text[]"`
		BountyReward     float64
		LootBoxReward    int
		DailyReward      int
		Bounty           string
		BountyTime       string
		UpdatedAt        time.Time `gorm:"not null;default:current_timestamp"`
	}

	Flower struct {
		ID                string `gorm:"primaryKey"`
		BountyRewards     int
		DisplayName       string
		UpdateTime        string
		DailyBeesSeen     pq.StringArray `gorm:"type:text[]"`
		FirstSeen         *string
		HBeesSeen         pq.StringArray `gorm:"type:text[]"`
		WalletAddress     string
		CoveredHexes      pq.StringArray `gorm:"type:text[]"`
		LastSeen          *string
		DailyAttaches     int
		H3Hex             string
		Lat               float64
		Lng               float64
		Address           string
		Suburb            string
		City              string
		State             string
		Town              string
		County            string
		Active            int
		FlowerRewards     float64
		DailyCoveredHexes pq.StringArray `gorm:"type:text[]"`
		NFTAddress        string
		Nickname          string
		FlowerAttaches    int
		DailyHBeesSeen    pq.StringArray `gorm:"type:text[]"`
		DailyRewards      float64
		ImageURL          string
		BeesSeen          string
		UpdatedAt         time.Time `gorm:"not null;default:current_timestamp"`
	}

	Reward struct {
		ID                string `gorm:"primaryKey"`
		PCN               float64
		PIC               float64
		RSERatio          float64
		Client            string
		Coverage          pq.StringArray `gorm:"type:text[]"`
		DailyPIC          float64
		Date              string
		Device            string
		DeviceType        string
		Reward            string
		Transaction       string
		TransactionStatus string
		Wallet            string
		UpdatedAt         time.Time `gorm:"not null;default:current_timestamp"`
	}
)

var (
	tableNameHex    = "pollen_hexes"
	tableNameFlower = "pollen_flowers"
	tableNameReward = "pollen_rewards"
	indexes         = []string{
		"CREATE INDEX IF NOT EXISTS idx_pollen_rewards_device ON pollen_rewards (device, date DESC)",
	}
	models = []interface{}{
		Hex{},
		Reward{},
		Flower{},
	}
	upsertClause = clause.OnConflict{
		Columns:   []clause.Column{{Name: "id"}},
		UpdateAll: true,
		DoUpdates: []clause.Assignment{{Column: clause.Column{Name: "updated_at"}, Value: time.Now()}},
	}
)

func (h *Hex) TableName() string {
	return tableNameHex
}

func (f *Flower) TableName() string {
	return tableNameFlower
}

func (r *Reward) TableName() string {
	return tableNameReward
}

func quietLogger() logger.Interface {
	return logger.New(
		log.New(os.Stdout, "\r\n", log.LstdFlags),
		logger.Config{
			SlowThreshold: time.Second * 5,
			LogLevel:      logger.Error,
			Colorful:      false,
		},
	)
}

// Pollen API
// (rate limit unknown)
//
var (
	pollenAPIHexes   = "https://api.pollenmobile.io/explorer/hexes?partial=true&h3_hex_top="
	pollenAPIHex     = "https://api.pollenmobile.io/explorer/hex?h3_hex="
	pollenAPIFlowers = "https://api.pollenmobile.io/explorer/flowers"
	pollenAPIRewards = "https://api.pollenmobile.io/explorer/device-rewards-all?device="
	// Includes NYC and some parts of neighboring cities
	pollenAPIHexesNYC = "852a1393fffffff,852a104bfffffff,852a1057fffffff,852a1063fffffff,852a100bfffffff,852a106ffffffff,852a13c3fffffff,852a107bfffffff,852a102ffffffff,852a1383fffffff,852a103bfffffff,852a1047fffffff,852a139bfffffff,852a106bfffffff,852a1077fffffff,852a12b7fffffff,852a102bfffffff,852a13d7fffffff,852a138bfffffff,852a1043fffffff,852a1397fffffff,852a104ffffffff,852a1003fffffff,852a1067fffffff,852a100ffffffff,852a12a7fffffff,852a1073fffffff,852a101bfffffff,852a13c7fffffff,852a12b3fffffff,852a13d3fffffff"
	// Includes SF and some neighboring cities
	// pollenAPIHexesCA_BayArea = "85283457fffffff,852830c7fffffff,85283467fffffff,8528346ffffffff,85283403fffffff,852830d7fffffff,85283477fffffff,8528340bfffffff,8528341bfffffff,85283083fffffff,8528342bfffffff,8528308bfffffff,85283093fffffff,8528343bfffffff,8528309bfffffff,85283443fffffff,85283453fffffff,852836a7fffffff,852830c3fffffff,85283463fffffff,852830cbfffffff,8528346bfffffff,852836b7fffffff,852830d3fffffff,85283473fffffff,852830dbfffffff,85283407fffffff,8528347bfffffff,8528340ffffffff,85283417fffffff,8528308ffffffff,85283447fffffff,8528344ffffffff"
	pollenAPIHeaders = map[string]string{
		"accept":     "application/json",
		"origin":     "https://explorer.pollenmobile.io",
		"referer":    "https://explorer.pollenmobile.io/",
		"user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36",
		"x-api-key":  "DNFN9tSZUI6xJFt7YiVsJ2Omk3fAiJ1nnOyo0Wp4",
	}
	pollenRateLimit     = ratelimit.New(1, ratelimit.Per(time.Millisecond*500))
	pollenRetries       = 3
	pollenRetryWaitTime = time.Minute * 3
)

type (
	// Pollen API Response for Hex Grid (for map)
	HexListItem struct {
		ID          string `json:"h3_hex"`
		Covered     int    `json:"covered"`
		FlowerCount int    `json:"flower_count"`
	}

	// Pollen API Response for Grid-specific Information
	HexItem struct {
		Hex struct {
			SignalStrength   int      `json:"signalStrength,string"`
			Attach           int      `json:"attach,string"`
			LastCovered      string   `json:"last_covered"`
			LastPollenDrop   string   `json:"last_pollen_drop"`
			Device           []string `json:"device"`
			Time             string   `json:"time"`
			H3HexTop         string   `json:"h3_hex_top"`
			Flowers          []string `json:"flowers"`
			FlowersContained []string `json:"flowers_contained"`
			BountyReward     float64  `json:"bountyReward,string"`
			H3Hex            string   `json:"h3_hex"`
			Ping             float64  `json:"ping,string"`
			LootBoxReward    int      `json:"lootBoxReward,string"`
			DailyReward      int      `json:"dailyReward,string"`
			Bounty           string   `json:"bounty"`
			BountyTime       string   `json:"bounty_time"`
		} `json:"hex"`
	}

	// Pollen API Response for Flowers
	FlowerListItem struct {
		BountyRewards     int               `json:"bounty_rewards,string"`
		DisplayName       string            `json:"displayname"`
		UpdateTime        string            `json:"update_time"`
		DailyBeesSeen     []string          `json:"daily_bees_seen"`
		FirstSeen         *string           `json:"first_seen"`
		HBeesSeen         []string          `json:"hbees_seen"`
		WalletAddress     string            `json:"wallet_address"`
		CoveredHexes      []string          `json:"covered_hexes"`
		LastSeen          *string           `json:"last_seen"`
		DailyAttaches     int               `json:"daily_attaches,string"`
		H3Hex             string            `json:"h3_hex"`
		Active            int               `json:"attach,string"`
		FlowerRewards     float64           `json:"flower_rewards,string"`
		DailyCoveredHexes []string          `json:"daily_covered_hexes"`
		NFTAddress        string            `json:"nft_address"`
		ID                string            `json:"flowerID"`
		Nickname          string            `json:"nickname"`
		FlowerAttaches    int               `json:"flower_attaches,string"`
		DailyHBeesSeen    []string          `json:"daily_hbees_seen"`
		DailyRewards      float64           `json:"daily_rewards,string"`
		ImageURL          string            `json:"image_url"`
		BeesSeen          map[string]string `json:"bees_seen"`
	}

	DeviceRewards    map[string][]DeviceRewardItem
	DeviceRewardItem struct {
		PCN      float64 `json:"PCN,string"`
		PIC      float64 `json:"PIC,string"`
		RSERatio float64 `json:"RSEratio,string"`
		Client   string  `json:"client"`
		// This should usually be []string, but some records mangle the API response
		// with "[]". So we'll have to fix that manually.
		Coverage          interface{} `json:"coverage"`
		DailyPIC          float64     `json:"dailyPIC,string"`
		Date              string      `json:"date"`
		Device            string      `json:"device"`
		DeviceType        string      `json:"device_type"`
		Reward            string      `json:"reward"`
		RewardID          string      `json:"rewardID"`
		Transaction       string      `json:"transaction"`
		TransactionStatus string      `json:"tx_status"`
		Wallet            string      `json:"wallet"`
	}
)

func getAllHexes(area string) ([]HexListItem, error) {
	return pollenAPICallWithRetries[[]HexListItem](pollenAPIHexes + area)
}

func getHexDetails(hex string) (HexItem, error) {
	return pollenAPICallWithRetries[HexItem](pollenAPIHex + hex)
}

func getAllFlowers() ([]FlowerListItem, error) {
	return pollenAPICallWithRetries[[]FlowerListItem](pollenAPIFlowers)
}

func getRewards(deviceName string) ([]DeviceRewardItem, error) {
	rewardsByDate, err := pollenAPICallWithRetries[DeviceRewards](pollenAPIRewards + deviceName)
	rewards := []DeviceRewardItem{}
	for _, dailyRewards := range rewardsByDate {
		rewards = append(rewards, dailyRewards...)
	}
	return rewards, err
}

func pollenAPICallWithRetries[T interface{}](url string) (t T, err error) {
	for i := 0; i < pollenRetries; i++ {
		t, err := pollenAPICall[T](url)
		if err == nil {
			return t, err
		}
		time.Sleep(pollenRetryWaitTime)
	}
	return t, err
}

func pollenAPICall[T interface{}](url string) (t T, err error) {
	pollenRateLimit.Take()

	cli := http.Client{Timeout: time.Second * 60}
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return t, err
	}
	for key, value := range pollenAPIHeaders {
		req.Header.Set(key, value)
	}
	res, err := cli.Do(req)
	if err != nil {
		return t, err
	}
	defer res.Body.Close()
	err = json.NewDecoder(res.Body).Decode(&t)
	return
}

// OSM (Nominatim) API for reverse geocoding lat/lngs
// (rate limit: 1/s)
//
var (
	osmAPI = func(lat, lng float64) string {
		return fmt.Sprintf("https://nominatim.openstreetmap.org/reverse?lat=%v&lon=%v&format=json", lat, lng)
	}
	osmCache     = map[string]ReverseGeocode{}
	osmUA        = "pollen"
	osmRateLimit = ratelimit.New(1) // 1/s
)

type (
	OSMPAPIResponse struct {
		DisplayName string `json:"display_name"`
		Address     struct {
			Suburb string `json:"suburb"`
			City   string `json:"city"`
			State  string `json:"state"`
			Town   string `json:"town"`
			County string `json:"county"`
		} `json:"address"`
	}

	ReverseGeocode struct {
		Lat     float64
		Lng     float64
		Address string
		Suburb  string
		City    string
		State   string
		Town    string
		County  string
	}
)

func initGeocodeCache(db *gorm.DB) error {
	tables := []struct {
		name   string
		column string
	}{
		{name: tableNameHex, column: "id"},
		{name: tableNameFlower, column: "h3_hex"},
	}
	for _, table := range tables {
		var records []struct {
			Hex string
			ReverseGeocode
		}
		err := db.
			Table(table.name).
			Select(fmt.Sprintf(
				"%s AS Hex, lat AS Lat, lng AS Lng, address AS address, suburb AS Suburb, city AS City, state AS State, town AS Town, county AS County",
				table.column,
			)).
			Find(&records).Error
		if err != nil {
			return err
		}
		for _, record := range records {
			osmCache[record.Hex] = record.ReverseGeocode
		}
	}
	return nil
}

func reverseGeocode(hex string) (ReverseGeocode, error) {
	if record, ok := osmCache[hex]; ok {
		return record, nil
	}
	osmRateLimit.Take()

	lat, lng := hexToLatLng(hex)
	cli := http.Client{Timeout: time.Second * 60}
	req, err := http.NewRequest(http.MethodGet, osmAPI(lat, lng), nil)
	if err != nil {
		return ReverseGeocode{}, err
	}
	req.Header.Set("user-agent", osmUA)
	res, err := cli.Do(req)
	if err != nil {
		return ReverseGeocode{}, err
	}
	defer res.Body.Close()
	var place OSMPAPIResponse
	err = json.NewDecoder(res.Body).Decode(&place)
	if err != nil {
		return ReverseGeocode{}, err
	}
	g := ReverseGeocode{
		Lat:     lat,
		Lng:     lng,
		Address: place.DisplayName,
		Suburb:  place.Address.Suburb,
		City:    place.Address.City,
		State:   place.Address.State,
		Town:    place.Address.Town,
		County:  place.Address.County,
	}
	osmCache[hex] = g
	return g, nil
}

func hexToLatLng(hex string) (float64, float64) {
	value, _ := strconv.ParseInt(hex, 16, 64)
	latLng := h3.CellToLatLng(h3.Cell(value))
	return latLng.Lat, latLng.Lng
}

// Helpers
//
func isValidHex(s string) bool {
	hexes := strings.Split(s, ",")
	for _, hex := range hexes {
		if len(hex) != 15 {
			return false
		}
		_, err := strconv.ParseInt(hex, 16, 64)
		if err != nil {
			return false
		}
	}
	return true
}

func handleErr(err error) {
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		panic(err)
	}
}
