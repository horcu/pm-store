package v1

import (
	"context"
	firebase "firebase.google.com/go"
	"firebase.google.com/go/db"
	"fmt"
	models "github.com/horcu/mafia-models"
	"github.com/joho/godotenv"
	"google.golang.org/api/option"
	"log"
	"os"
	"sync"
)

// Publisher Firebase
type Publisher struct {
	*db.Client
	mu sync.Mutex
}

var pub Publisher

const (
	firebaseDBURL = "https://peez-mafia-427718-default-rtdb.firebaseio.com/"
)

func (db *Publisher) Connect() error {
	ctx := context.Background()

	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	// Get Firebase config from environment variables
	firebaseConfigFile := os.Getenv("FIREBASE_CONFIG_FILE")
	if firebaseConfigFile == "" {
		return fmt.Errorf("FIREBASE_CONFIG_FILE environment variable not set")
	}

	opt := option.WithCredentialsFile(firebaseConfigFile)
	config := &firebase.Config{DatabaseURL: firebaseDBURL}
	app, err := firebase.NewApp(ctx, config, opt)
	if err != nil {
		return fmt.Errorf("error initializing app: %v", err)
	}
	client, err := app.Database(ctx)
	if err != nil {
		return fmt.Errorf("error initializing database: %v", err)
	}
	db.Client = client
	return nil
}

func FirebaseDB() *Publisher {
	return &pub
}

type Store struct {
	*Publisher
}

func (store *Store) Connect() error {
	store.mu.Lock()
	defer store.mu.Unlock()
	return store.Publisher.Connect()
}

// NewStore returns a Store.
func NewStore() *Store {
	d := FirebaseDB()
	st := &Store{
		Publisher: d,
	}

	return st
}

func (store *Store) Create(b interface{}, path string) error {
	switch path {
	case "players":
		return store.createPlayer(b.(*models.Player))
	case "game_groups":
		return store.createGameGroup(b.(*models.Group))
	case "games":
		return store.createGame(b.(*models.Game))
	case "steps":
		return store.createStep(b.(*models.Step))
	case "characters":
		return store.createCharacter(b.(*models.Character))
	case "abilities":
		return store.createAbility(b.(*models.Ability))
	default:
		return fmt.Errorf("invalid data type: %s", path)
	}
}

func (store *Store) createStep(b *models.Step) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := store.NewRef("steps/"+b.Bin).Set(context.Background(), &b); err != nil {
		return err
	}
	return nil
}

func (store *Store) createGame(b *models.Game) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := store.NewRef("games/"+b.Bin).Set(context.Background(), b); err != nil {
		return err
	}
	return nil
}

func (store *Store) createGameGroup(b *models.Group) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := store.NewRef("game_groups/"+b.Bin).Set(context.Background(), b); err != nil {
		return err
	}
	return nil
}

func (store *Store) createPlayer(b *models.Player) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := store.NewRef("players/"+b.Bin).Set(context.Background(), b); err != nil {
		return err
	}
	return nil
}

func (store *Store) Delete(b interface{}, dataType string) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	switch dataType {
	case "players":
		return store.deletePlayer(b.(*models.Player))
	case "game_groups":
		return store.deleteGameGroup(b.(*models.Group))
	case "games":
		return store.deleteGame(b.(*models.Game))
	default:
		return fmt.Errorf("invalid data type: %s", dataType)
	}
}

func (store *Store) deleteGame(b interface{}) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	return store.NewRef("games/" + b.(*models.Game).Bin).Delete(context.Background())
}

func (store *Store) deleteGameGroup(b interface{}) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	return store.NewRef("game_groups/" + b.(*models.Group).Bin).Delete(context.Background())
}

func (store *Store) deletePlayer(b interface{}) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	if b == nil {
		return fmt.Errorf("invalid player object")
	}
	return store.NewRef("players/" + b.(*models.Player).Bin).Delete(context.Background())
}

func (store *Store) GetByBin(b string, dataType string) (interface{}, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	var t interface{}
	if dataType == "players" {
		t = &models.Player{}
	} else if dataType == "game_groups" {
		t = &models.Group{}
	} else if dataType == "games" {
		t = &models.Game{}
	} else if dataType == "characters" {
		t = &models.Character{}
	} else if dataType == "abilities" {
		t = &models.Ability{}
	} else {
		return nil, fmt.Errorf("invalid data type: %s", dataType)
	}

	if err := store.NewRef(dataType+"/"+b).Get(context.Background(), t); err != nil {
		return nil, err
	}

	return t, nil
}

func (store *Store) Update(b string, m map[string]interface{}, path string) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	switch path {
	case "players":
		return store.updatePlayer(b, m)
	case "game_groups":
		return store.updateGameGroup(b, m)
	case "games":
		return store.updateGame(b, m)
	case "gamers":
		return store.updateGamersInGame(b, m)
	default:
		return fmt.Errorf("invalid data type: %s", path)
	}
}

func (store *Store) updateGame(b string, m map[string]interface{}) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := store.NewRef("games/"+b).Update(context.Background(), m); err != nil {
		return err
	}
	return nil
}

func (store *Store) updateGameGroup(b string, m map[string]interface{}) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := store.NewRef("game_groups/"+b).Update(context.Background(), m); err != nil {
		return err
	}
	return nil
}

func (store *Store) updatePlayer(b string, m map[string]interface{}) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := store.NewRef("players/"+b).Update(context.Background(), m); err != nil {
		return err
	}
	return nil
}

func (store *Store) addInvitationToPlayer(playerId string, bin string, m *models.Invitation) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	err := store.NewRef("players/"+playerId+"/invitations/"+bin).Set(context.Background(), m)
	if err != nil {
		return err
	}

	return nil
}

func (store *Store) addPlayerToGroupMembers(gId string, bin string, m *models.Player) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	err := store.NewRef("game_groups/"+gId+"/members/"+bin).Set(context.Background(), m)
	if err != nil {
		return err
	}

	return nil
}

func (store *Store) addInvitationToGame(gameId string, m map[string]interface{}) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	_, err := store.NewRef("games/"+gameId+"/invitations").Push(context.Background(), m)
	if err != nil {
		return err
	}

	return nil
}

func (store *Store) getAllPlayers() ([]*models.Player, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	var m interface{}
	if err := store.NewRef("players/").Get(context.Background(), &m); err != nil {
		return nil, err
	}
	// convert m to a list of players
	var players []*models.Player
	for _, v := range m.(map[string]interface{}) {
		p := v.(map[string]interface{})
		players = append(players, &models.Player{
			Bin:      p["bin"].(string),
			UserName: p["user_name"].(string),
			Status:   p["status"].(string),
			Photo:    p["photo"].(string),
			Privacy:  p["privacy"].(string),
		})
	}
	return players, nil
}

func (store *Store) getGameByBin(bin string) (*models.Game, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	var g *models.Game
	if err := store.NewRef("games/"+bin).Get(context.Background(), &g); err != nil {

		return nil, err
	}
	return g, nil
}

func (store *Store) getStepByBin(bin string) (*models.Step, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	var st *models.Step
	if err := store.NewRef("steps/"+bin).Get(context.Background(), st); err != nil {
		return nil, err
	}
	return st, nil
}

func (store *Store) getAllGroups() ([]*models.Group, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	var m interface{}
	if err := store.NewRef("game_groups/").Get(context.Background(), &m); err != nil {
		return nil, err
	}

	// convert m to a list of Groups
	var groups []*models.Group
	for _, v := range m.(map[string]interface{}) {
		g := v.(map[string]interface{})
		groups = append(groups, &models.Group{
			Bin:       g["bin"].(string),
			Creator:   g["creator"].(*models.Player),
			Members:   g["members"].([]*models.Player),
			GroupName: g["group_name"].(string),
			Capacity:  int(g["capacity"].(float64)),
			Status:    g["status"].(string),
		})
	}
	return groups, nil
}

func (store *Store) getAllSteps() ([]*models.Step, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	var m interface{}
	if err := store.NewRef("steps").Get(context.Background(), &m); err != nil {
		return nil, err
	}

	// convert m to a list of steps
	var steps []*models.Step
	for _, v := range m.([]interface{}) {
		var st = v.(map[string]interface{})
		steps = append(steps, &models.Step{
			Bin:          st["bin"].(string),
			StepType:     st["step_type"].(string),
			Duration:     st["duration"].(string),
			Command:      st["command"].(string),
			Characters:   st["characters"].(map[string]*models.Character),
			StepIndex:    int(st["step_index"].(float64)),
			SubSteps:     st["sub_steps"].(map[string]*models.Step),
			RequiresVote: st["requires_vote"].(bool),
			VoteType:     st["vote_type"].(string),
			Allowed:      st["allowed"].([]string),
			NextStep:     st["next_step"].(string),
		})
	}
	return steps, nil
}

func (store *Store) getGameGroup(bin string) (*models.Group, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	var g *models.Group
	if err := store.NewRef("game_groups/"+bin).Get(context.Background(), g); err != nil {
		return nil, err
	}
	return g, nil
}

func (store *Store) getPlayer(bin string) (*models.Player, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	var p *models.Player
	if err := store.NewRef("players/"+bin).Get(context.Background(), p); err != nil {
		return nil, err
	}
	return p, nil
}

func (store *Store) getAllGames() ([]*models.Game, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	var m interface{}
	if err := store.NewRef("games/").Get(context.Background(), &m); err != nil {
		return nil, err
	}

	// Dereference the pointer to the interface
	gamesMap := m.(map[string]interface{})

	if len(gamesMap) == 0 {
		var l = make([]*models.Game, 0)
		return l, nil
	}
	// convert m to a list of games
	var games []*models.Game
	for _, v := range gamesMap {
		g := v.(map[string]interface{})

		// Convert the "invited" slice
		invited := store.parsePlayerList(g, "invited")

		// Convert the "current step" object
		step := store.parseCurrentStep(g, "current_step")

		// Convert the group object
		group := store.parseGroup(g, "game_group")

		// Convert the creator object
		creator := store.parsePlayer(g, "creator")

		var invitedList []string
		for _, player := range invited {
			invitedList = append(invitedList, player.Bin)
		}

		// add parsed Game to list
		games = append(games, &models.Game{
			Bin:               g["bin"].(string),
			IsDaytime:         g["is_daytime"].(bool),
			FirstDayCompleted: g["first_day_completed"].(bool),
			CurrentStep:       step.Bin,
			GroupId:           group.Bin,
			Status:            g["status"].(string),
			ServerName:        g["server_name"].(string),
			ServerAddress:     g["server_ip"].(string),
			ServerPort:        int(g["server_port"].(float64)),
			StartTime:         g["start_time"].(string),
			EndTime:           g["end_time"].(string),
			Creator:           creator,
			Invited:           invitedList,
		})
	}
	return games, nil
}

func (store *Store) parsePlayerList(pMap map[string]interface{}, path string) []*models.Player {
	store.mu.Lock()
	defer store.mu.Unlock()
	var players []*models.Player

	if pMap[path] == nil {
		return make([]*models.Player, 0)
	}

	interF := pMap[path].([]interface{})
	if len(interF) == 0 {
		return make([]*models.Player, 0)
	}

	for _, playerInterface := range interF {
		playerMap := playerInterface.(map[string]interface{})
		players = append(players, &models.Player{
			Bin:      playerMap["bin"].(string),
			UserName: playerMap["user_name"].(string),
			Status:   playerMap["status"].(string),
			Photo:    playerMap["photo"].(string),
			Privacy:  playerMap["privacy"].(string),
		})
	}
	return players
}

func (store *Store) parseCurrentStep(pMap interface{}, path string) *models.Step {
	store.mu.Lock()
	defer store.mu.Unlock()
	var step *models.Step

	playerMap := pMap.(map[string]interface{})
	st := playerMap[path].(map[string]interface{})
	step = &models.Step{
		Bin:          st["bin"].(string),
		StepType:     st["step_type"].(string),
		Duration:     st["duration"].(string),
		Command:      st["command"].(string),
		Characters:   st["characters"].(map[string]*models.Character),
		StepIndex:    int(st["step_index"].(float64)),
		SubSteps:     st["sub_steps"].(map[string]*models.Step),
		RequiresVote: st["requires_vote"].(bool),
		VoteType:     st["vote_type"].(string),
		Allowed:      st["allowed"].([]string),
		NextStep:     st["next_step"].(string),
	}
	return step
}

func (store *Store) parseGroup(pMap interface{}, path string) *models.Group {
	store.mu.Lock()
	defer store.mu.Unlock()
	var step *models.Group

	playerMap := pMap.(map[string]interface{})
	interF := playerMap[path].(map[string]interface{})
	step = &models.Group{
		Bin:       interF["bin"].(string),
		Creator:   store.parsePlayer(interF, "creator"),
		Members:   store.parsePlayerList(interF, "members"),
		GroupName: interF["group_name"].(string),
		Capacity:  int(interF["capacity"].(float64)),
		Status:    interF["status"].(string),
	}
	return step
}

func (store *Store) parsePlayer(g interface{}, path string) *models.Player {
	store.mu.Lock()
	defer store.mu.Unlock()
	var group *models.Player

	playerMap := g.(map[string]interface{})
	if playerMap[path] == nil {
		return &models.Player{}
	}
	interF := playerMap[path].(map[string]interface{})
	group = &models.Player{
		Bin:      interF["bin"].(string),
		UserName: interF["user_name"].(string),
		Status:   interF["status"].(string),
		Photo:    interF["photo"].(string),
		Privacy:  interF["privacy"].(string),
	}
	return group
}

func (store *Store) parseInvitationList(pMap map[string]interface{}, path string) ([]*models.Invitation, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	var accepted []*models.Invitation
	acceptedInterface := pMap[path].([]interface{})

	if len(acceptedInterface) == 0 {
		var l = make([]*models.Invitation, 0)
		return l, nil
	}

	for _, playerInterface := range acceptedInterface {
		playerMap := playerInterface.(map[string]interface{})
		accepted = append(accepted, &models.Invitation{
			Bin:        playerMap["bin"].(string),
			GameGroup:  playerMap["game_group"].(string),
			CreatorId:  playerMap["creator_id"].(string),
			Status:     playerMap["status"].(string),
			Invitation: playerMap["invitation"].(string),
			Message:    playerMap["message"].(string),
			Time:       playerMap["time"].(string),
			GameId:     playerMap["game_id"].(string),
			Accepted:   playerMap["accepted"].(bool),
			Declined:   playerMap["declined"].(bool),
		})
	}
	return accepted, nil
}

func (store *Store) getGameGroupMembers(groupId string) ([]*models.Player, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	var m interface{}
	if err := store.NewRef("game_groups/"+groupId+"/members").Get(context.Background(), &m); err != nil {
		return nil, err
	}

	// convert m to a list of players
	var players []*models.Player
	for _, v := range m.(map[string]interface{}) {
		p := v.(map[string]interface{})
		players = append(players, &models.Player{
			Bin:      p["bin"].(string),
			UserName: p["user_name"].(string),
			Status:   p["status"].(string),
			Photo:    p["photo"].(string),
			Privacy:  p["privacy"].(string),
		})
	}
	return players, nil
}

func (store *Store) getGameGroupInvitations(groupId string) ([]*models.Invitation, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	var m interface{}
	if err := store.NewRef("game_groups/"+groupId+"/invitations").Get(context.Background(), &m); err != nil {
		return nil, err
	}

	// convert m to a list of invitations
	var invitations []*models.Invitation
	for _, v := range m.(map[string]interface{}) {
		inv := v.(map[string]interface{})
		invitations = append(invitations, &models.Invitation{
			Bin:       inv["bin"].(string),
			GameGroup: inv["game_group"].(string),
			CreatorId: inv["creator"].(string),
		})
	}
	return invitations, nil
}

func (store *Store) getStepsByGameId(gameId string) ([]*models.Step, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	var m interface{}
	if err := store.NewRef("games/"+gameId+"/steps").Get(context.Background(), &m); err != nil {
		return nil, err
	}

	// convert m to a list of steps
	var steps []*models.Step
	for _, v := range m.(map[string]interface{}) {
		st := v.(map[string]interface{})
		steps = append(steps, &models.Step{
			Bin:          st["bin"].(string),
			StepType:     st["step_type"].(string),
			Duration:     st["duration"].(string),
			Command:      st["command"].(string),
			Characters:   st["characters"].(map[string]*models.Character),
			StepIndex:    int(st["step_index"].(float64)),
			SubSteps:     st["sub_steps"].(map[string]*models.Step),
			RequiresVote: st["requires_vote"].(bool),
			VoteType:     st["vote_type"].(string),
			Allowed:      st["allowed"].([]string),
			NextStep:     st["next_step"].(string),
		})
	}
	return steps, nil
}

func (store *Store) updateInvitation(pId string, inviteId string, m map[string]interface{}) interface{} {
	store.mu.Lock()
	defer store.mu.Unlock()
	err := store.NewRef("players/"+pId+"/invitations/"+inviteId).Update(context.Background(), m)
	if err != nil {
		return err
	}

	return nil

}

func (store *Store) createCharacter(character *models.Character) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := store.NewRef("characters/"+character.Bin).Set(context.Background(), character); err != nil {
		return err
	}
	return nil
}

func (store *Store) AddStepToGame(step *models.Step, id string) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := store.NewRef("games/"+id+"/steps/"+step.Bin).Set(context.Background(), step); err != nil {
		return err
	}
	return nil
}

func (store *Store) updateGamersInGame(b string, m map[string]interface{}) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	err := store.NewRef("games/"+b+"gamers").Update(context.Background(), m)
	if err != nil {
		return err
	}

	return nil

}

func (store *Store) SetGameStartAndEndTimes(gameId string, startTime string, endTime string) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	m := map[string]interface{}{
		"start_time": startTime,
		"end_time":   endTime,
	}

	err := store.NewRef("games/"+gameId).Update(context.Background(), m)
	if err != nil {
		return err
	}

	return nil
}

func (store *Store) addToGame(path string, bin string, c *models.Character) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := store.NewRef("games/"+bin+"/"+path+"/"+c.Bin).Set(context.Background(), &c); err != nil {
		return err
	}
	return nil
}

func (store *Store) createAbility(ability *models.Ability) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := store.NewRef("abilities/"+ability.Bin).Set(context.Background(), ability); err != nil {
		return err
	}
	return nil
}

func (store *Store) SetGameFirstStep(bin string, step string) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := store.NewRef("games/"+bin+"/current_step/").Set(context.Background(), step); err != nil {
		return err
	}
	return nil
}

func (store *Store) ResetFirstDayAndExplanationFlag(bin string) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := store.NewRef("games/"+bin+"/first_day_completed/").Set(context.Background(), false); err != nil {
		return err
	}
	if err := store.NewRef("games/"+bin+"/explanation_seen/").Set(context.Background(), false); err != nil {
		return err
	}
	return nil
}

func (store *Store) AddActionToGamer(gameId string, gamerId string, stepId string, a *models.Action) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := store.NewRef("games/"+gameId+"/gamers/"+gamerId+"/actions/"+stepId).Set(context.Background(), a); err != nil {
		return err
	}

	return nil
}

func (store *Store) GetStepByBin(step string) (*models.Step, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	c := &models.Step{}
	if err := store.NewRef("steps/"+step).Get(context.Background(), c); err != nil {
		return nil, err
	}
	if c.Bin == "" {
		return nil, nil
	}
	return c, nil
}

func (store *Store) getCharacterByBin(id string) (*models.Character, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	c := &models.Character{}
	if err := store.NewRef("characters/"+id).Get(context.Background(), c); err != nil {
		return nil, err
	}
	if c.Bin == "" {
		return nil, nil
	}
	return c, nil
}

func (store *Store) UpdateVoteStep(gameBin string, stepBin string, updateStep map[string]interface{}) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	return store.NewRef("games/"+gameBin+"/steps/"+stepBin+"/sub_steps/233fcfa6-bf62-4262-b9c5-823f32e32ef3").Update(context.Background(), updateStep)
}

func (store *Store) UpdateGamer(gameId string, gamerId string, gx map[string]interface{}) bool {
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := store.NewRef("games/"+gameId+"/gamers/").Update(context.Background(), gx); err != nil {
		return false
	}
	return true
}
