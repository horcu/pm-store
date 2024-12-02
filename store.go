package v1

import (
	"context"
	"errors"
	firebase "firebase.google.com/go"
	"firebase.google.com/go/db"
	"fmt"
	"github.com/google/uuid"
	models "github.com/horcu/pm-models/types"
	"github.com/joho/godotenv"
	"google.golang.org/api/option"
	"log"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"time"
)

// Publisher Firebase
type Publisher struct {
	*db.Client
	mu sync.Mutex
}

var pub Publisher

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

	firebaseDBURL := os.Getenv("FIREBASE_URL")
	if firebaseConfigFile == "" {
		return fmt.Errorf("FIREBASE_URL environment variable not set")
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
		return store.CreatePlayer(b.(*models.Player))
	case "games":
		return store.CreateGame(b.(*models.Game))
	case "steps":
		return store.CreateStep(b.(*models.Step))
	case "characters":
		return store.CreateCharacter(b.(*models.GameCharacter))
	case "abilities":
		return store.CreateAbility(b.(*models.Ability))
	default:
		return fmt.Errorf("invalid data type: %s", path)
	}
}

func (store *Store) CreateStep(b *models.Step) error {
	store.mu.Lock()
	if err := store.NewRef("steps/"+b.Bin).Set(context.Background(), &b); err != nil {
		return err
	}
	store.mu.Unlock()
	return nil
}

func (store *Store) CreateGame(b *models.Game) error {
	store.mu.Lock()
	if err := store.NewRef("games/"+b.Bin).Set(context.Background(), b); err != nil {
		return err
	}
	store.mu.Unlock()
	return nil
}

func (store *Store) CreatePlayer(b *models.Player) error {

	if err := store.NewRef("players/"+b.Bin).Set(context.Background(), b); err != nil {
		return err
	}
	return nil
}

func (store *Store) Delete(b interface{}, dataType string) error {

	switch dataType {
	case "players":
		return store.DeletePlayer(b.(*models.Player))
	case "game_groups":
		return store.DeleteGameGroup(b.(*models.Group))
	case "games":
		return store.DeleteGame(b.(*models.Game))
	default:
		return fmt.Errorf("invalid data type: %s", dataType)
	}
}

func (store *Store) DeleteGame(b interface{}) error {

	return store.NewRef("games/" + b.(*models.Game).Bin).Delete(context.Background())
}

func (store *Store) DeleteGameGroup(b interface{}) error {

	return store.NewRef("game_groups/" + b.(*models.Group).Bin).Delete(context.Background())
}

func (store *Store) DeletePlayer(b interface{}) error {

	if b == nil {
		return fmt.Errorf("invalid player object")
	}
	return store.NewRef("players/" + b.(*models.Player).Bin).Delete(context.Background())
}

func (store *Store) GetByBin(b string, dataType string) (interface{}, error) {

	var t interface{}
	if dataType == "players" {
		t = &models.Player{}
	} else if dataType == "game_groups" {
		t = &models.Group{}
	} else if dataType == "games" {
		t = &models.Game{}
	} else if dataType == "characters" {
		t = &models.GameCharacter{}
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

func (store *Store) GetGamerByBin(b string, gId string) (*models.Gamer, error) {

	var t *models.Gamer
	if err := store.NewRef("games/"+gId+"/gamers/"+b).Get(context.Background(), &t); err != nil {
		return nil, err
	}

	return t, nil
}

func (store *Store) Update(b string, m map[string]interface{}, path string) error {

	switch path {
	case "players":
		return store.UpdatePlayer(b, m)
	case "game_groups":
		return store.UpdateGameGroup(b, m)
	case "games":
		return store.UpdateGame(b, m)
	case "gamers":
		return store.UpdateGamersInGame(b, m)
	default:
		return fmt.Errorf("invalid data type: %s", path)
	}
}

func (store *Store) UpdateGame(b string, m map[string]interface{}) error {
	if err := store.NewRef("games/"+b).Update(context.Background(), m); err != nil {
		return err
	}
	return nil
}

func (store *Store) UpdateGameGroup(b string, m map[string]interface{}) error {
	if err := store.NewRef("game_groups/"+b).Update(context.Background(), m); err != nil {
		return err
	}
	return nil
}

func (store *Store) UpdatePlayer(b string, m map[string]interface{}) error {
	if err := store.NewRef("players/"+b).Update(context.Background(), m); err != nil {
		return err
	}
	return nil
}

func (store *Store) AddInvitationToPlayer(playerId string, bin string, m *models.Invitation) error {

	err := store.NewRef("players/"+playerId+"/invitations/"+bin).Set(context.Background(), m)
	if err != nil {
		return err
	}

	return nil
}

func (store *Store) AddPlayerToGroupMembers(gId string, bin string, m *models.Player) error {

	err := store.NewRef("game_groups/"+gId+"/members/"+bin).Set(context.Background(), m)
	if err != nil {
		return err
	}

	return nil
}

func (store *Store) AddInvitationToGame(gameId string, m map[string]interface{}) error {

	_, err := store.NewRef("games/"+gameId+"/invitations").Push(context.Background(), m)
	if err != nil {
		return err
	}

	return nil
}

func (store *Store) GetAllPlayers() ([]*models.Player, error) {

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

	var g *models.Game
	if err := store.NewRef("games/"+bin).Get(context.Background(), &g); err != nil {

		return nil, err
	}
	return g, nil
}

func (store *Store) getAllGroups() ([]*models.Group, error) {

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
			Members:   g["members"].(map[string]*models.Player),
			GroupName: g["group_name"].(string),
			Capacity:  int(g["capacity"].(float64)),
			Status:    g["status"].(string),
		})
	}
	return groups, nil
}

func (store *Store) getAllSteps() ([]*models.Step, error) {

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
			Characters:   st["characters"].(map[string]*models.GameCharacter),
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

	var g *models.Group
	if err := store.NewRef("game_groups/"+bin).Get(context.Background(), g); err != nil {
		return nil, err
	}
	return g, nil
}

func (store *Store) getPlayer(bin string) (*models.Player, error) {

	var p *models.Player
	if err := store.NewRef("players/"+bin).Get(context.Background(), p); err != nil {
		return nil, err
	}
	return p, nil
}

func (store *Store) getAllGames() ([]*models.Game, error) {

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
		invited := store.ParsePlayerList(g, "invited")

		// Convert the "current step" object
		step := store.ParseCurrentStep(g, "current_step")

		// Convert the group object
		//group := store.ParseGroup(g, "game_group")

		// Convert the creator object
		creator := store.ParsePlayer(g, "creator")

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
			Info:              g["info"].(*models.ServerInfo),
			Status:            g["status"].(string),
			StartTime:         g["start_time"].(string),
			EndTime:           g["end_time"].(string),
			Creator:           creator,
		})
	}
	return games, nil
}

func (store *Store) ParsePlayerList(pMap map[string]interface{}, path string) map[string]*models.Player {

	var players map[string]*models.Player

	if pMap[path] == nil {
		return make(map[string]*models.Player, 0)
	}

	interF := pMap[path].([]interface{})
	if len(interF) == 0 {
		return make(map[string]*models.Player, 0)
	}

	for _, playerInterface := range interF {

		playerMap := playerInterface.(map[string]interface{})
		var p = &models.Player{
			Bin:      playerMap["bin"].(string),
			UserName: playerMap["user_name"].(string),
			Status:   playerMap["status"].(string),
			Photo:    playerMap["photo"].(string),
			Privacy:  playerMap["privacy"].(string),
		}
		players[p.Bin] = p
	}
	return players
}

func (store *Store) ParseCurrentStep(pMap interface{}, path string) *models.Step {

	var step *models.Step

	playerMap := pMap.(map[string]interface{})
	st := playerMap[path].(map[string]interface{})
	step = &models.Step{
		Bin:          st["bin"].(string),
		StepType:     st["step_type"].(string),
		Duration:     st["duration"].(string),
		Command:      st["command"].(string),
		Characters:   st["characters"].(map[string]*models.GameCharacter),
		StepIndex:    int(st["step_index"].(float64)),
		SubSteps:     st["sub_steps"].(map[string]*models.Step),
		RequiresVote: st["requires_vote"].(bool),
		VoteType:     st["vote_type"].(string),
		Allowed:      st["allowed"].([]string),
		NextStep:     st["next_step"].(string),
	}
	return step
}

func (store *Store) ParseGroup(pMap interface{}, path string) *models.Group {

	var step *models.Group

	playerMap := pMap.(map[string]interface{})
	interF := playerMap[path].(map[string]interface{})
	step = &models.Group{
		Bin:       interF["bin"].(string),
		Creator:   store.ParsePlayer(interF, "creator"),
		Members:   store.ParsePlayerList(interF, "members"),
		GroupName: interF["group_name"].(string),
		Capacity:  int(interF["capacity"].(float64)),
		Status:    interF["status"].(string),
	}
	return step
}

func (store *Store) ParsePlayer(g interface{}, path string) *models.Player {

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

func (store *Store) ParseInvitationList(pMap map[string]interface{}, path string) ([]*models.Invitation, error) {

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

func (store *Store) GetGameGroupMembers(groupId string) ([]*models.Player, error) {

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

func (store *Store) GetGameGroupInvitations(groupId string) ([]*models.Invitation, error) {

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

func (store *Store) GetStepsByGameId(gameId string) ([]*models.Step, error) {

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
			Characters:   st["characters"].(map[string]*models.GameCharacter),
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

func (store *Store) UpdateInvitation(pId string, inviteId string, m map[string]interface{}) interface{} {
	err := store.NewRef("players/"+pId+"/invitations/"+inviteId).Update(context.Background(), m)
	if err != nil {
		return err
	}

	return nil

}

func (store *Store) CreateCharacter(character *models.GameCharacter) error {

	if err := store.NewRef("characters/"+character.Bin).Set(context.Background(), character); err != nil {
		return err
	}
	return nil
}

func (store *Store) AddStepToGame(step *models.Step, id string) error {

	if err := store.NewRef("games/"+id+"/steps/"+step.Bin).Set(context.Background(), step); err != nil {
		return err
	}
	return nil
}

func (store *Store) UpdateGamersInGame(b string, m map[string]interface{}) error {
	err := store.NewRef("games/"+b+"gamers").Update(context.Background(), m)
	if err != nil {
		return err
	}

	return nil

}

func (store *Store) SetGameStartAndEndTimes(gameId string, startTime string, endTime string) error {

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

func (store *Store) AddToGame(path string, bin string, c *models.GameCharacter) error {
	if err := store.NewRef("games/"+bin+"/"+path+"/"+c.Bin).Set(context.Background(), &c); err != nil {
		return err
	}
	return nil
}

func (store *Store) CreateAbility(ability *models.Ability) error {

	if err := store.NewRef("abilities/"+ability.Bin).Set(context.Background(), ability); err != nil {
		return err
	}
	return nil
}

func (store *Store) SetGameFirstStep(bin string, step string) error {

	if err := store.NewRef("games/"+bin+"/current_step/").Set(context.Background(), step); err != nil {
		return err
	}
	return nil
}

func (store *Store) ResetFirstDayAndExplanationFlag(bin string) error {

	if err := store.NewRef("games/"+bin+"/first_day_completed/").Set(context.Background(), false); err != nil {
		return err
	}
	if err := store.NewRef("games/"+bin+"/explanation_seen/").Set(context.Background(), false); err != nil {
		return err
	}
	return nil
}

func (store *Store) GetStepByBin(step string) (*models.Step, error) {

	c := &models.Step{}
	if err := store.NewRef("steps/"+step).Get(context.Background(), c); err != nil {
		return nil, err
	}
	if c.Bin == "" {
		return nil, nil
	}
	return c, nil
}

func (store *Store) GetCharacterByBin(id string) (*models.GameCharacter, error) {

	c := &models.GameCharacter{}
	if err := store.NewRef("characters/"+id).Get(context.Background(), c); err != nil {
		return nil, err
	}
	if c.Bin == "" {
		return nil, nil
	}
	return c, nil
}

func (store *Store) UpdateVoteStep(gameBin string, stepBin string, updateStep map[string]interface{}) error {

	return store.NewRef("games/"+gameBin+"/steps/"+stepBin).Update(context.Background(), updateStep)
}

func (store *Store) UpdateGamer(gameId string, gx map[string]interface{}) bool {
	if err := store.NewRef("games/"+gameId+"/gamers/").Update(context.Background(), gx); err != nil {
		return false
	}
	return true
}

func (store *Store) UpdateGamerAbilities(gameId string, gamerId string, gx map[string]interface{}) bool {
	if err := store.NewRef("games/"+gameId+"/gamers/"+gamerId+"/abilities/").Update(context.Background(), gx); err != nil {
		return false
	}
	return true
}

func (store *Store) AddAbilitiesToDb(abilities map[string]*models.Ability) error {

	for _, a := range abilities {
		err := store.Create(a, "abilities")
		if err != nil {
			return err
		}
	}
	return nil
}

func (store *Store) AddAbilitiesToGame(gameId string, abilities map[string]*models.Ability) error {
	if err := store.NewRef("games/"+gameId+"/abilities").Set(context.Background(), &abilities); err != nil {
		return err
	}
	return nil
}

func (store *Store) GetAbilitiesForCharacter(characterId string) ([]*models.Ability, error) {

	var abilities []*models.Ability
	character, err := store.GetByBin(characterId, "characters")
	if err != nil {
		return nil, err
	}

	var parsedChar = character.(*models.GameCharacter)
	for _, ability := range parsedChar.Abilities {
		ab, err := store.GetByBin(ability.Bin, "abilities")
		if err != nil {
			break
		}
		ability := ab.(*models.Ability)
		abilities = append(abilities, ability)
	}

	return abilities, nil
}

func (store *Store) SetNewStep(gameId string) {

	// find game
	game, err := store.GetByBin(gameId, "games")
	if err != nil {
		return
	}

	//parse game into a Game struct object
	g := game.(*models.Game)

	// set the game's current step
	g.CurrentStep = "1"

	// update the game
	err = store.Update(gameId, map[string]interface{}{
		"current_step": g.CurrentStep,
	}, "games")
	if err != nil {
		return
	}

	return
}

func (store *Store) SetNextStep(gameId string) {

	// find game
	game, err := store.GetByBin(gameId, "games")
	if err != nil {
		return
	}

	//parse game into a Game struct object
	g := game.(*models.Game)

	// get the current step
	currentStep, err := store.GetStepByBin(g.CurrentStep)
	if err != nil {
		return
	}

	// set the game's current step
	g.CurrentStep = currentStep.Bin

	// update the game
	err = store.Update(gameId, map[string]interface{}{
		"current_step": g.CurrentStep,
	}, "games")
	if err != nil {
		return
	}

	return
}

func (store *Store) AddAllCharactersToDb(chars map[string]*models.GameCharacter) error {

	for _, s := range chars {
		//s.Bin = strconv.Itoa(i)
		err := store.Create(s, "characters")
		if err != nil {
			return err
		}
	}
	return nil
}

func (store *Store) AddAllCharactersToGame(gameId string, chars map[string]*models.GameCharacter) error {

	if err := store.NewRef("games/"+gameId+"/characters/").Set(context.Background(), chars); err != nil {
		return err
	}

	return nil
}

func (store *Store) AddAllStepsToDb(chars map[string]*models.Step) error {

	for _, s := range chars {
		//s.Bin = strconv.Itoa(i)
		err := store.Create(s, "steps")
		if err != nil {
			return err
		}
	}
	return nil
}

func (store *Store) InitializeGame(game *models.Game) {

	err := store.Create(&game, "games")
	if err != nil {
		return
	}
}

func (store *Store) AddRandomUsers(userNames []string, photoUrls []string) (bool, error) {

	// generate users
	for _, un := range userNames {
		err := store.Create(&models.Player{
			UserName: un,
			Bin:      uuid.New().String(),
			Photo:    photoUrls[rand.Intn(len(photoUrls))],
			Status:   "available",
			Privacy:  "public",
		}, "player")
		if err != nil {
			return false, err
		}
	}

	return true, nil
}

func (store *Store) CreateGameGroup(groupName string, cap int, ownerId string, userIds []string) (bool, error) {

	// find all users and build a user object for each
	var users map[string]*models.Player
	for _, uId := range userIds {
		var u, _ = store.GetByBin(uId, "players")
		user := u.(*models.Player)
		users[user.Bin] = user
	}

	owner, _ := store.GetByBin(ownerId, "players")

	// create a group
	err := store.Create(&models.Group{
		Bin:       uuid.New().String(),
		Creator:   owner.(*models.Player),
		Members:   users,
		GroupName: groupName,
		Capacity:  cap,
		Status:    "waiting",
	}, "game_groups")
	if err != nil {
		return false, err
	}

	return true, nil

}

func (store *Store) AddPlayerToGroup(playerId string, groupId string) {

	// find a game group
	gameGroup, err := store.GetByBin(groupId, "game_groups")
	if err != nil {
		return
	}

	// parse game group into a Group struct object
	g := gameGroup.(*models.Group)

	// find player
	player, err := store.GetByBin(playerId, "players")
	if err != nil {
		return
	}

	// parse player into a Player struct object
	p := player.(*models.Player)

	// add player to the game group's members array
	g.Members[p.Bin] = p

	// update the game group
	err = store.Update(groupId, map[string]interface{}{
		"members": g.Members,
	}, "game_groups")
	if err != nil {
		return
	}

	return
}

func (store *Store) RemovePlayerFromGroup(playerId string, groupId string) {

	// find a group
	gameGroup, err := store.GetByBin(groupId, "game_groups")
	if err != nil {
		return
	}

	// parse game group into a Group struct object
	g := gameGroup.(*models.Group)

	// remove player from the game group's members array
	for _, m := range g.Members {
		if m.Bin == playerId {
			// remove player  m from g.Members
			delete(g.Members, m.Bin)
			break
		}
	}

	// update the game group
	err = store.Update(groupId, map[string]interface{}{
		"members": g.Members,
	}, "game_groups")
	if err != nil {
		return
	}

	return
}

func (store *Store) InvitePlayerToGroup(playerId string, invitation *models.Invitation) {

	// find player
	player, err := store.GetByBin(playerId, "players")
	if err != nil {
		return
	}

	// parse player into a Player struct object
	p := player.(*models.Player)

	// push invitation to player's invitation list
	err = store.AddInvitationToPlayer(p.Bin, invitation.Bin, invitation)
	if err != nil {
		return
	}

	return
}

func (store *Store) InvitePlayerToGame(playerId string, invitation models.Invitation) (bool, error) {

	// update the player
	err := store.AddInvitationToPlayer(playerId, invitation.Bin, &invitation)
	if err != nil {
		return false, err
	}

	//  find player
	p, err := store.GetByBin(playerId, "players")
	if err != nil {
		return false, err
	}

	//  convert
	plr := p.(models.Player)

	//add the invitation to the player's list of invites
	err = store.AddInvitationToPlayer(plr.Bin, invitation.Bin, &invitation)
	if err != nil {
		return false, err
	}

	return true, nil
}

func (store *Store) AcceptGameInvitation(playerId string, invitation *models.Invitation) (bool, error) {

	// update the player's invitations' list to include this invitation
	err := store.Update(playerId, map[string]interface{}{
		"invitations": []models.Invitation{
			{
				Bin:        invitation.Bin,
				GameGroup:  invitation.GameGroup,
				CreatorId:  invitation.CreatorId,
				Status:     "received", // created //received //replied
				Invitation: "game",
				Message:    "I'm down!",
				Time:       "",
				GameId:     invitation.GameId,
				Accepted:   true,
				Declined:   false,
			},
		},
	}, "players")
	if err != nil {
		return false, err
	}

	return true, nil
}

func (store *Store) DeclineGameInvitation(playerId string, invitation *models.Invitation) (bool, error) {

	// update the player's invitations' list to include this invitation
	err := store.Update(playerId, map[string]interface{}{
		"invitations": []*models.Invitation{
			{
				Bin:        invitation.Bin,
				GameGroup:  invitation.GameGroup,
				CreatorId:  invitation.CreatorId,
				Status:     "received", // created //received //replied
				Invitation: "game",
				Message:    "I'm not down!",
				Time:       "",
				GameId:     invitation.GameId,
				Accepted:   false,
				Declined:   true,
			},
		},
	}, "players")
	if err != nil {
		return false, err
	}

	return true, nil
}

func (store *Store) AcceptGroupInvitation(p *models.Player, invitationId string, groupId string) (bool, error) {

	// update the invitation record
	for i, inv := range p.Invitations {
		if inv.Bin == invitationId {
			if inv.Accepted {
				return false, errors.New("invitation already accepted")
			}
			if inv.Declined {
				return false, errors.New("invitation already declined")
			}

			p.Invitations[i].Accepted = true
			p.Invitations[i].Status = "accepted"

			m := map[string]interface{}{
				"accepted": true,
				"declined": false,
			}

			store.UpdateInvitation(p.Bin, invitationId, m)
			break
		}
	}

	// Use a map to check for existing group ID
	groupIdsMap := make(map[string]bool)
	for _, id := range p.GroupIds {
		groupIdsMap[id] = true
	}

	if !groupIdsMap[groupId] {
		p.GroupIds = append(p.GroupIds, groupId)
	}

	//update the player group ids
	err := store.Update(p.Bin, map[string]interface{}{
		"group_ids": p.GroupIds,
	}, "players")
	if err != nil {
		return false, err
	}

	// find game_group
	gameGroup, err := store.GetByBin(groupId, "game_groups")
	if err != nil {
		return false, err
	}

	// parse game group into a Group struct object
	g := gameGroup.(*models.Group)

	// add player to the member list
	err = store.AddPlayerToGroupMembers(groupId, g.Bin, p)
	if err != nil {
		return false, err
	}

	return true, nil
}

func (store *Store) DeclineGameGroupInvitation(p *models.Player, invitationId string, groupId string) {

	// find the invitation by id
	for i, inv := range p.Invitations {
		if inv.Bin == invitationId {
			p.Invitations[i].Declined = true
			p.Invitations[i].Accepted = false
			p.Invitations[i].Status = "declined"
			break
		}
	}

	// update the player
	err := store.Update(p.Bin, map[string]interface{}{
		"invitations": p.Invitations,
	}, "players")
	if err != nil {
		return
	}

	// remove player from group member list if they previously aceepted
	gameGroup, err := store.GetByBin(groupId, "game_groups")
	if err != nil {
		return
	}

	// parse game group into a Group struct object
	g := gameGroup.(*models.Group)

	// remove player from the game group's members array
	for _, m := range g.Members {
		if m.Bin == p.Bin {
			delete(g.Members, p.Bin)
			break
		}
	}

	// update the game group
	err = store.Update(groupId, map[string]interface{}{
		"members": g.Members,
	}, "game_groups")
	if err != nil {
		return
	}

	return
}

func (store *Store) AddStepsToGame(steps map[string]*models.Step, gameId string) map[string]*models.Step {

	for _, s := range steps {
		err := store.AddStepToGame(s, gameId)
		if err != nil {
			return nil
		}
	}
	return steps
}

func (store *Store) StartGame(gameId string) (bool, error) {

	// find game
	game, err := store.GetByBin(gameId, "games")
	if err != nil {
		return false, err
	}

	//parse game into a Game struct object
	g := game.(*models.Game)

	// set the game's status to start
	g.Status = "started"

	// update the game
	err = store.Update(gameId, map[string]interface{}{
		"status": g.Status,
	}, "games")
	if err != nil {
		return false, err
	}

	return true, nil
}

func (store *Store) EndGame(gameId string) (bool, error) {

	// find game
	game, err := store.GetByBin(gameId, "games")
	if err != nil {
		return false, err
	}

	//parse game into a Game struct object
	g := game.(*models.Game)

	// set the game's status to ended
	g.Status = "ended"

	//send command to agones to kill the server

	//  after the previous step is successful update the game
	err = store.Update(gameId, map[string]interface{}{
		"status": g.Status,
	}, "games")
	if err != nil {
		return false, err
	}

	return true, nil
}

func (store *Store) Vote(vote *models.Vote) bool {
	store.mu.Lock()
	defer store.mu.Unlock()

	log.Printf("voting")
	// get the current step from the game's list of  steps
	game, err := store.getGameByBin(vote.GameBin)
	if err != nil {
		log.Printf("Error getting game data: %v", err)
		return false
	}

	// add vote action to the Results map for that step and the current cycle
	var mp = buildStepResult(game, vote.Source, vote)

	log.Printf("updating game")
	//update the result in the game
	err = store.UpdateGame(game.Bin, *mp)
	if err != nil {
		return false
	}

	// check if the bot's character is alive
	log.Printf("voted")
	return true

}

func buildStepResult(game *models.Game, gamerId string, action *models.Vote) *map[string]interface{} {
	var stamp = strconv.FormatInt(time.Now().UnixMilli(), 10)

	if game.Steps[game.CurrentStep].Result == nil {
		game.Steps[game.CurrentStep].Result = make(map[string][]*models.Result)
	}

	//ensure there is at least one entry
	//if res := game.Steps[game.CurrentStep].Result[gamerId]; res == nil {
	//	// new entry
	//	game.Steps[game.CurrentStep].Result[gamerId] = []*models.Result{}
	//}

	//set the step history
	game.Steps[game.CurrentStep].Result[gamerId] = append(game.Steps[game.CurrentStep].Result[gamerId], &models.Result{
		Bin:       uuid.New().String(),
		StepBin:   action.StepBin,
		GameBin:   game.Bin,
		GamerId:   gamerId,
		TimeStamp: stamp,
		Vote:      *action,
	})

	//build update map
	return &map[string]interface{}{
		"steps/" + game.CurrentStep + "/result/" + gamerId: game.Steps[game.CurrentStep].Result[gamerId],
	}
}

func (store *Store) ArchiveStepResults(gameId string) error {
	// get the game
	g, err := store.GetByBin(gameId, "games")
	if err != nil {
		return err
	}

	game := g.(*models.Game)

	if game.StepResults == nil {
		game.StepResults = make(map[string][]*models.Result)
	}

	for _, step := range game.Steps {
		// check is the step has the result node first
		if step.Result != nil {
			// add the step's results to the game's result node with the gamer's bin from the result as the key
			for _, result := range step.Result {
				for _, res := range result {
					var r = game.StepResults[res.GamerId]
					r = append(r, res)
				}
			}
		}
	}

	//then remove all results from all game steps
	for _, step := range game.Steps {
		step.Result = nil
	}

	//publish the changes to the game node
	err = store.Update(gameId, map[string]interface{}{
		"steps":        game.Steps,
		"step_results": game.StepResults,
	}, "games")
	if err != nil {
		return err
	}

	return nil
}

func (store *Store) ApplyAbility(abilityBin string, gameBin string, targetGamer string) {
	// construct a models.Fate struct from the ability
	fate := &models.Fate{
		Bin:        uuid.New().String(),
		AbilityBin: abilityBin,
	}

	// add the fate to the targetGamer
	err := store.NewRef("games/"+gameBin+"/gamers/"+targetGamer+"/fate").Set(context.Background(), fate)
	if err != nil {
		log.Printf("Error adding fate to gamer: %v", err)
	}
}

func (store *Store) AddMessageToGame(msg *models.Message, gameId string) error {
	if err := store.NewRef("games/"+gameId+"/messages/").Set(context.Background(), msg); err != nil {
		return err
	}
	return nil
}

func (store *Store) GetPlayerToken(bin string) (*string, error) {

	var token *string
	if err := store.NewRef("players/"+bin+"/token").Get(context.Background(), &token); err != nil {
		return nil, err
	}
	return token, nil
}
