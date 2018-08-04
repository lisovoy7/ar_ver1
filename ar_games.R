library(foreach)
library(doParallel)
registerDoParallel(4)
getDoParWorkers() 
setwd("/home/andrey.lisovoy/TEMP_ANDREY/Arena/")

ArenaRewardsDistConf <- as.data.table(read_delim("~/TEMP_ANDREY/Arena/arena_rewards_dist_conf.csv", "\t", escape_double = FALSE, trim_ws = TRUE))
ArenaWheelConfiguration <- as.data.table(read_delim("~/TEMP_ANDREY/Arena/arena_wheel_configuration.csv", "\t", escape_double = FALSE, trim_ws = TRUE))
ArenaBoardConfiguration <- as.data.table(read_delim("~/TEMP_ANDREY/Arena/arena_board_configuration.csv", "\t", escape_double = FALSE, trim_ws = TRUE))

ArenaBoardConfiguration <- merge(ArenaBoardConfiguration , ArenaRewardsDistConf[is.tile.reward==1], by= c("chapter_id", "board_id"), all.x = TRUE,all.y = FALSE)
ArenaBoardConfiguration[ , reward_tiles_multiplier := reward_tiles_weight / sum(reward_tiles_weight, na.rm = T) , .(board_set_id, chapter_id, board_id)]
ArenaRewardsDistConf[ , weight_prcnt := weight/sum(weight)]
                     
board.set.id <- 2
num.iter <- 10
mystery.type.prob <- c(0.25,0.15,0.25,0.15,0.20)

ArenaPreCompGames <- data.table()
ptime <- system.time({
for(wheel.id in unique(ArenaWheelConfiguration$wheel_id)) {
  wheel_config <- ArenaWheelConfiguration[wheel_id==wheel.id, c("steps", "probability")]
  out.info <- data.table()
  out <- foreach(iter = 1:num.iter, .combine=rbind) %dopar% {
    tile.id <- 0
    board.id <- 1
    chapter.id <- 1
    board_config <- ArenaBoardConfiguration[board_set_id==board.set.id & chapter_id==chapter.id & board_id==board.id]
    boardLength <- max(board_config$tile_id)
    is.finished <- FALSE
    turns_spent <- 0
    extra_turns_left <- 0
    
    while( !is.finished ) {
      
      board_reward_tiles_multiplier <- 0
      board_reward_prcnt <- 0
      chapter_reward_prcnt <- 0
      grand_reward_prcnt <- 0

      if(tile.id==max(board_config$tile_id)){
        tile.id <- 0
        board.id <- 1 + board.id %% (max(ArenaRewardsDistConf$board_id, na.rm = TRUE))
        if(board.id==1){ ## means user just completed board 3 and starts now board 1 of next chapter
          reward_chapter_multiplier <- 1 ## since new chapter has started, it should get multiplier=1
          chapter.id <- 1 + chapter.id %% (max(ArenaRewardsDistConf$chapter_id, na.rm = TRUE))
        }
        board_config <- ArenaBoardConfiguration[board_set_id==board.set.id & chapter_id==chapter.id & board_id==board.id]
        boardLength <- max(board_config$tile_id)
      }

      turns_spent <- ifelse(extra_turns_left==0, turns_spent + 1, turns_spent)
      if(extra_turns_left>0){extra_turns_left <- extra_turns_left-1}
      
      tutorial_scripted_moves <- board_config[tile_id==tile.id, tutorial_scripted_moves]
      steps <- ifelse( !is.na(tutorial_scripted_moves), tutorial_scripted_moves, sample(wheel_config$steps, 1, prob = wheel_config$probability))
      prev.tile.id <- tile.id
      tile.id <- min(tile.id+steps, max(board_config$tile_id))

      item_type <- board_config[tile_id==tile.id, item_type_id]

      ## Let's check if we have passed Milestone (not including landing tile itself since it will be checked after on, regardless)
      if(nrow(board_config[item_type_id==2 & between(tile_id, prev.tile.id+1, tile.id-1)])>0){
        board_reward_tiles_multiplier <- board_config[item_type_id==2 & between(tile_id, prev.tile.id+1, tile.id-1), sum(reward_tiles_multiplier)] 
        last_board_reward_tiles_multiplier <- board_config[item_type_id==2 & between(tile_id, prev.tile.id+1, tile.id-1), .SD[.N, reward_tiles_multiplier]] 
        out.info <- rbind(out.info , data.table("wheel_id" = wheel.id, iter, steps, turns_spent, "tile_id" = tile.id, "board_id" = board.id, "chapter_id" = chapter.id, extra_turns_left, "item_type" = 2, reward_chapter_multiplier, board_reward_tiles_multiplier, board_reward_prcnt, chapter_reward_prcnt, grand_reward_prcnt))
        board_reward_tiles_multiplier <- 0
        }
      
      if(item_type == 1){
        board_reward_tiles_multiplier <- board_config[tile_id==tile.id, sum(reward_tiles_multiplier)]
        last_board_reward_tiles_multiplier <- board_reward_tiles_multiplier
        }
      
      if(item_type == 2){
        board_reward_tiles_multiplier <- board_config[tile_id==tile.id, sum(reward_tiles_multiplier)]
        last_board_reward_tiles_multiplier <- board_reward_tiles_multiplier
        }
      
      if(item_type == 3){
          mystery.type.id <- sample(1:length(mystery.type.prob), 1, prob = mystery.type.prob)
          if (mystery.type.id==1) { ## More turns
            extra_turns_left <- board_config[tile_id==tile.id, reward_free_turns_amount]
            item_type <- 3.1
          } else if (mystery.type.id==2) { ## Jump to next reward
            tile.id <- board_config[tile_id>tile.id & item_type_id %in% c(1,2), min(tile_id, boardLength)]
            board_reward_tiles_multiplier <- board_config[tile_id==tile.id & item_type_id %in% c(1,2), reward_tiles_multiplier]
            item_type <- 3.2
          } else if (mystery.type.id==3) { ## Adding more items
            reward_items_amount <- ArenaBoardConfiguration[chapter_id==chapter.id & board_id==board.id & tile_id==tile.id, reward_items_amount]
            more_items <- rbinom(n = 1, size = ceil((boardLength-tile.id)/wheel_config[ , sum(steps * probability)]), p = reward_items_amount / boardLength)
            board_reward_tiles_multiplier <- more_items * board_config[tile_id==tile.id, reward_tiles_multiplier]
            item_type <- 3.3
          } else if (mystery.type.id==4) { ## Change the Chapter reward
            reward_chapter_multiplier <- reward_chapter_multiplier * board_config[tile_id==tile.id, reward_chapter_multiplier]
            item_type <- 3.4
          } else if (mystery.type.id==5) { ## Retake the last reward
            board_reward_tiles_multiplier <- last_board_reward_tiles_multiplier
            item_type <- 3.5
          } else {print("Unknown Mystery type")}
        }
      
      if(tile.id >= boardLength){
        board_reward_prcnt <- board_reward_prcnt + ArenaRewardsDistConf[chapter_id==chapter.id & board_id==board.id & is.tile.reward==0, weight_prcnt]
        if(board.id==3){
          chapter_reward_prcnt <- chapter_reward_prcnt + reward_chapter_multiplier * ArenaRewardsDistConf[chapter_id==chapter.id & is.na(board_id) & is.tile.reward==0, weight_prcnt]
          if(chapter.id==3){
            grand_reward_prcnt <- grand_reward_prcnt + ArenaRewardsDistConf[is.na(chapter_id) & is.na(board_id) & is.tile.reward==0, weight_prcnt]
            is.finished <- TRUE
            }
          }
        }
      out.info <- rbind(out.info , data.table("wheel_id" = wheel.id, iter, steps, turns_spent, "tile_id" = tile.id, "board_id" = board.id, "chapter_id" = chapter.id, extra_turns_left, item_type, reward_chapter_multiplier, board_reward_tiles_multiplier, board_reward_prcnt, chapter_reward_prcnt, grand_reward_prcnt))
    }
    out.info
  }
  ArenaPreCompGames <- rbind(ArenaPreCompGames, out) 
}})

stopImplicitCluster()
print(ptime[3])

ArenaPreCompGames[order(iter, turns_spent, chapter_id, board_id, tile_id), prev_turns_spent := shift(turns_spent, 1, type='lag', fill=0) , .(wheel_id, iter)]
ArenaPreCompGames[order(iter, turns_spent, chapter_id, board_id, tile_id), turns_spent_across_iter := cumsum(turns_spent-prev_turns_spent), .(wheel_id)]
ArenaPreCompGames$prev_turns_spent <- NULL

ArenaPreCompGames[order(iter, turns_spent, chapter_id, board_id, tile_id), boards_completed := lapply(.SD, function(x) cumsum(ifelse(x==4, 1, 0))), by=.(iter, wheel_id), .SDcols = "item_type"]
ArenaPreCompGames[ , sub_turn := 1:.N  , .(wheel_id, iter, board_id, chapter_id, turns_spent, tile_id)]

saveRDS(ArenaPreCompGames, file = "ArenaPreCompGames.rds")
