library(RODBC)
library(data.table)
library(dplyr)
library(sqldf)
library(matrixStats)
library(RcppRoll)
library(DBI)
library(odbc)

Sys.time()

setwd("/home/andrey.lisovoy/TEMP_ANDREY/Arena/")

connImpala <- "Impala"
connAnalysis <- paste("Driver=ODBC Driver 13 for SQL Server; Server=10.145.16.22; Database=Analysis;    Uid=r_reader; Pwd=hIDGPJ4ioW2R5AaW7ap7" ,sep='' )

con <- dbConnect(odbc::odbc() , driver="ODBC Driver 13 for SQL Server" ,server="10.145.16.22" ,database="Analysis", uid="r_rw" , pwd="hIDGPJ4ioW2R5AaW7ap7")  

impala.query = function (channel.name , query){
  channel <- odbcConnect(channel.name)
  dt <- data.table(sqlQuery(channel, query, stringsAsFactors = FALSE))
  odbcClose(channel)
  dt
}

impala.save = function (impala.schema, impala.table, df){
  channel.name <- dbConnect(odbc::odbc() , "Impala",  schema=impala.schema) 
  dbWriteTable(channel.name, impala.table, df[1 : min(1024 , nrow(df)) ] , overwrite=T) 
  i=1
  while(i < ceil(nrow(df)/1024) ){
    dbWriteTable(channel.name, impala.table, df[ (1024*i+1) : min(1024*(i+1) , nrow(df)) ], append=T)
    i <- i+1
  }
  dbDisconnect(channel.name)
}

hbipa.query = function (channel.name , query){
  channel <- odbcDriverConnect(channel.name)
  dt <- data.table(sqlQuery(channel, query, stringsAsFactors = FALSE))
  odbcClose(channel)
  dt
}

find_best_wheel = function(turns){
  min.wager.turns.promo <- promo.duration*turns_per_meter*min.wager.bar.fill.daily
  as.numeric(TurnsToCompletePerWheel[pmin(TurnsToCompletePerWheel$turns_to_complete - min.wager.turns.promo, 0.85 * TurnsToCompletePerWheel$turns_to_complete) > turns
                                                            , max(wheel_id, min(TurnsToCompletePerWheel$wheel_id))])
  }


find_bord_tiles_reward = function(arena.all.rewards, chapter.id, board.id, wheel){
  target_reward <- arena.all.rewards * ArenaRewardsDistConf[chapter_id==chapter.id & board_id==board.id & is.tile.reward==1, reward.prcnt]
  total_board_reward_tiles_multiplier <- TilesRewardFromSim[wheel.id==wheel & chapter_id==chapter.id & board_id==board.id, total_board_reward_tiles_multiplier]
  target_reward / total_board_reward_tiles_multiplier
  }  


ArenaWheelConfiguration <- as.data.table(read_delim("~/TEMP_ANDREY/Arena/arena_wheel_configuration.csv", "\t", escape_double = FALSE, trim_ws = TRUE))
ArenaWeelEV <- ArenaWheelConfiguration[ , .(ev = sum(steps * probability)) , wheel_id]

ArenaPreCompGames <- readRDS(file = "ArenaPreCompGames.rds")
TurnsToCompletePerWheel <- ArenaPreCompGames[item_type==4 & board_id==3 & chapter_id==3, .(turns_to_complete = ceil(mean(turns_spent))), .(wheel_id)]

TilesRewardFromSim <- ArenaPreCompGames[, .(total_board_reward_tiles_multiplier = sum(board_reward_tiles_multiplier, na.rm=T)/uniqueN(iter)), .(wheel_id, chapter_id, board_id)]
names(TilesRewardFromSim)[names(TilesRewardFromSim) == "wheel_id"] <- "wheel.id"

ArenaRewardsDistConf <- as.data.table(read_delim("~/TEMP_ANDREY/Arena/arena_rewards_dist_conf.csv", "\t", escape_double = FALSE, trim_ws = TRUE))
ArenaRewardsDistConf[ , reward.prcnt := weight / sum(weight)]

max_cycles <- 10
min.arena.all.rewards <- 200000 ## 40$ purchase OR 5 by 10$
min.wager.bar.fill.daily <- 2
turns.tutorial <- 2
turns_per_meter <- 3
play_again_grand_reward_mult <- 1.2
uplift <- 0.2
arena_reward_on_purchase <- 0.8
promo.duration <- 14
hist.duration <- 30 ##365
buffer.hist.to.sim <- 0

shift_days <- 100
decay_ratio <- 0.07
min_decay <- 0.5
floor_perc <- 0.5

sim.date.start <- as.Date('2018-04-27')
sim.dateid.start = as.integer(gsub("-", "", sim.date.start ))
sim.dateid.end = as.integer(gsub("-", "", (sim.date.start+(promo.duration-1)) ))
seg.dateid.start = as.integer(gsub("-", "", (sim.date.start-(hist.duration+buffer.hist.to.sim) )))
seg.dateid.end = as.integer(gsub("-", "", (sim.date.start-(buffer.hist.to.sim+1) )))
seg.date.end = sim.date.start-(buffer.hist.to.sim+1) 
sim.month.start = as.numeric(substr(as.character(sim.dateid.start),1,6))
sim.month.end = as.numeric(substr(as.character(sim.dateid.end),1,6))
seg.syssnapshotdateid.end = as.integer(gsub("-", "", (sim.date.start-(buffer.hist.to.sim))))
seg.month.start <- as.numeric(substr(as.character(seg.dateid.start),1,6))
seg.month.end <- as.numeric(substr(as.character(seg.dateid.end),1,6))

PriceListTable <- as.data.table(read_delim("~/TEMP_ANDREY/Arena/arena_price_list.csv", "\t", escape_double = FALSE, trim_ws = TRUE))
impala.save("andrey", "arena_price_list", PriceListTable[,c("denom","turns")])

LevelDenom <- hbipa.query(connHBIPA , paste("
select level_id as gamelevelid, offer_coins_multiplier_base
from [HOF-HBIP].[sources].[dbo].[MRR_level] 
where platform_type_id=1 and group_id=1"))

ArenaPers <- impala.query(connImpala , paste("
                            with
                            trns as(
                              select userid, date_ts, dateid, gross_amount, turns, moffercoinsamount, ",(1-floor_perc), "*( 1/(1+exp(",decay_ratio,"*(datediff(",paste0("'",seg.date.end,"'"),", date_ts)-",shift_days,"))) )+",floor_perc,"  as decay
                              from(
                                select userid, to_date(ts) as date_ts, dateid, sum(mamount) as gross_amount, sum(turns) as turns, sum(moffercoinsamount) as moffercoinsamount
                                from dwh.dwh_fact_transaction as a
                                join andrey.arena_price_list as b on round(a.mamount)=b.denom
                                where transtatusid=1 and triggerid !=18 and dateid between ",seg.dateid.start, " and ", seg.dateid.end, "
                                group by 1,2,3
                              ) as tmp
                            )
                            ,extreme_gross_amount as(
                              select userid, date_ts, decay, gross_amount as gross_amount_hist, gross_amount_decay, turns as turns_purchase_hist, turns_decay as turns_purchase_hist_decay, moffercoinsamount_decay as moffercoinsamount_hist_decay, d.dateid as dateid_from, tmp3.dateid as dateid_to
                              from(
                                select userid, date_ts, dateid, decay, gross_amount, gross_amount_decay, turns, turns_decay, moffercoinsamount_decay
                                from(
                                  select userid, date_ts, dateid, decay, gross_amount, decay*gross_amount as gross_amount_decay, turns, decay*turns as turns_decay, decay*moffercoinsamount as moffercoinsamount_decay
                                          , row_number() over (partition by userid order by decay*turns desc) as rn
                                  from(
                                    select a.userid, a.date_ts, a.dateid, min(a.decay) as decay, sum(b.gross_amount) as gross_amount, sum(b.turns) as turns, sum(b.moffercoinsamount) as moffercoinsamount
                                    from trns a 
                                    JOIN trns b on a.userid=b.userid and b.date_ts between date_add(a.date_ts, 1-(", promo.duration, ")) and a.date_ts 
                                    group by 1,2,3
                                  ) as tmp
                                ) as tmp2
                                where rn=1
                              ) as tmp3
                              join dwh.dwh_dim_date as d on date_add(tmp3.date_ts, 1-(", promo.duration, "))=d.dateshort 
                            )
                            select b.*, nvl(wager_hist_decay,0) as wager_hist_decay, nvl(h.gamelevelid, 25) as gamelevelid
                            from(
                              select a.userid, sum(b.mtotalbetsamount*decay) as wager_hist_decay
                              from extreme_gross_amount as a
                              left join dwh.dwh_fact_spin_agg as b on a.userid=b.userid and b.dateid between dateid_from and dateid_to and month>=",seg.month.start," and mtotalbetsamount>0
                              group by 1
                            ) as tmp
                            join extreme_gross_amount as b on b.userid=tmp.userid
                            left join dwh.dwh_dim_user_hist as h on h.userid=b.userid and syssnapshotdateid=" ,seg.syssnapshotdateid.end, "
                            order by 1"))

ArenaPers <- merge(ArenaPers, LevelDenom, by= c('gamelevelid'), all.x = TRUE, all.y = FALSE)
ArenaPers[ , turns_trans_effort := (1+uplift) * turns_purchase_hist_decay]

ArenaPers[ , wheel_id := sapply(turns_trans_effort, find_best_wheel)]
ArenaPers <- merge(ArenaPers , TurnsToCompletePerWheel, by.x= c('wheel_id'), by.y= c('wheel_id'), all.x = TRUE,all.y = FALSE)

ArenaPers[ , turns_wager_effort := pmax((turns_to_complete - turns_trans_effort), promo.duration * turns_per_meter * min.wager.bar.fill.daily)] ## we want guarantee at least few ['min.wager.bar.fill.daily'] fill-up of the 'wager-bar' per day ('wager-bar' awards user with few turns)
ArenaPers[ , effort_for_meter := pmax(5 * moffercoinsamount_hist_decay * offer_coins_multiplier_base, (wager_hist_decay / (turns_wager_effort / turns_per_meter)))]


ArenaPers[ , arena_all_rewards := pmax(min.arena.all.rewards *offer_coins_multiplier_base , arena_reward_on_purchase * (1+uplift) * moffercoinsamount_hist_decay * offer_coins_multiplier_base / pmax((turns_purchase_hist_decay + turns_wager_effort)/turns_to_complete, 1))]
ArenaPers[ , arena_initial_grand_reward := arena_all_rewards * ArenaRewardsDistConf[is.na(chapter_id) & is.na(board_id), reward.prcnt] ]
ArenaPers[ , arena_grand_reward := play_again_grand_reward_mult * arena_initial_grand_reward]

for(i in apply(ArenaRewardsDistConf[!is.na(chapter_id) & is.na(board_id), ], 1, as.list)){
  ArenaPers[ , paste0("chapter",i$chapter_id,"_grand_reward") := arena_all_rewards * i$reward.prcnt] }

for(i in apply(ArenaRewardsDistConf[!is.na(chapter_id) & !is.na(board_id) & is.tile.reward==0, ], 1, as.list)){
  ArenaPers[ , paste0("chapter",i$chapter_id,"_board",i$board_id,"_grand_reward") := arena_all_rewards * i$reward.prcnt] }

for(i in apply(ArenaRewardsDistConf[!is.na(chapter_id) & !is.na(board_id) & is.tile.reward==1, ], 1, as.list)){
  ArenaPers[ , paste0("chapter",i$chapter_id,"_board",i$board_id,"_tiles_reward") := mapply(find_bord_tiles_reward, arena_all_rewards, i$chapter_id, i$board_id, wheel_id)] }

saveRDS(ArenaPers, file = "ArenaPers.rds")



##################################################################################### SIMULATION ##########################################################################################
ArenaSim <- impala.query(connImpala , paste("
                                           with
                                           trans as(
                                                 select userid, gross_amount, turns_purchase, moffercoinsamount
                                                 from(
                                                 select userid, sum(mamount) as gross_amount, sum(turns) as turns_purchase, sum(moffercoinsamount) as moffercoinsamount
                                                 from dwh.dwh_fact_transaction as a
                                                 join andrey.arena_price_list as b on round(a.mamount)=b.denom
                                                 where transtatusid=1 and triggerid != 18 and dateid between ",sim.dateid.start, " and ", sim.dateid.end, "
                                                 group by 1
                                                 ) as tmp
                                               )
                                          ,wager as(
                                                 select userid, sum(wager) as wager, sum(case when spins > 50 then 1 else 0 end) as playing_days, max(trstierid) as trstierid 
                                                 from(
                                                   select userid, dateid, sum(mtotalbetsamount) as wager, sum(mbetscount) as spins, max(trstierid) as trstierid 
                                                   from dwh.dwh_fact_spin_agg 
                                                   where month>=",seg.month.start," and dateid between ",sim.dateid.start, " and ", sim.dateid.end, " and mtotalbetsamount>0 
                                                   group by 1,2
                                                 ) as tmp
                                                 group by 1
                                               )
                                           select wager.userid, nvl(trstierid, -1) as trstierid, nvl(wager,0) as wager_sim, nvl(playing_days,1) as playing_days, nvl(gross_amount,0) as gross_amount_sim , nvl(turns_purchase,0) as turns_purchase_sim, nvl(moffercoinsamount,0) as moffercoinsamount_sim, ",turns.tutorial, " as turns_tutorial_sim
                                           from wager
                                           left join trans on wager.userid=trans.userid"))

ArenaSim[ , isActiveDepositor := ifelse(gross_amount_sim > 20 , 1, 0)]
ArenaSim <- merge(ArenaPers, ArenaSim, by = c('userid'), all.x = FALSE,all.y = TRUE)
ArenaSim[ , turns_wager_sim := ceil(turns_per_meter * (wager_sim / effort_for_meter))]
ArenaSim[ , turns_total_sim := turns_purchase_sim + turns_wager_sim + turns_tutorial_sim]
ArenaSim[ , iter := sample((max(ArenaPreCompGames$iter) - max_cycles), nrow(ArenaSim), replace = T)]
ArenaSim <- merge(ArenaSim, ArenaPreCompGames[ , .(turns_spent_across_iter_from = min(turns_spent_across_iter)), .(wheel_id, iter)], by = c('wheel_id', 'iter'), all.x = TRUE,all.y = FALSE)

tmp <- ArenaSim[!is.na(turns_total_sim)]
ArenaSimRaw <- as.data.table(sqldf(paste0("
SELECT userid, a.wheel_id, gamelevelid, arena_all_rewards, playing_days, isActiveDepositor, turns_purchase_sim, turns_wager_sim, turns_total_sim,  chapter_id, board_id, item_type
          , max(boards_completed) as boards_completed, sum(board_reward_tiles_multiplier) as board_reward_tiles_multiplier, sum(board_reward_prcnt) as board_reward_prcnt, sum(chapter_reward_prcnt) as chapter_reward_prcnt, sum(grand_reward_prcnt) as grand_reward_prcnt, count(*) 
FROM tmp as a 
LEFT JOIN ArenaPreCompGames as b on a.wheel_id = b.wheel_id 
                                     and b.iter >= a.iter 
                                     and b.iter <  a.iter + ", max_cycles, "
                                     and b.turns_spent_across_iter >= a.turns_spent_across_iter_from 
                                     and b.turns_spent_across_iter <  a.turns_spent_across_iter_from + a.turns_total_sim
GROUP BY userid, a.wheel_id, gamelevelid, arena_all_rewards, playing_days, isActiveDepositor, turns_purchase_sim, turns_wager_sim, turns_total_sim, chapter_id, board_id, item_type")))


for(i in apply(ArenaRewardsDistConf[!is.na(chapter_id) & !is.na(board_id), ], 1, as.list)){
  ArenaSimRaw[chapter_id == i$chapter_id & board_id == i$board_id, tile_reward_sim := get(paste0("chapter",i$chapter_id,"_board",i$board_id,"_tiles_reward")) * board_reward_tiles_multiplier]
  }
ArenaSimRaw[ , board_reward_sim := board_reward_prcnt * arena_all_rewards]
ArenaSimRaw[ , chapter_reward_sim := chapter_reward_prcnt * arena_all_rewards]
ArenaSimRaw[ , grand_reward_sim := grand_reward_prcnt * arena_all_rewards]



setkey(ArenaSimRaw, userid, turns_spent)
saveRDS(ArenaSimRaw, file = "ArenaSimRaw.rds")



dbWriteTable(con, "arena_turns_acquired", ArenaSimRaw[userid==1341 , `:=` (boards_completed=max(boards_completed), daily_turns_purchase_sim=round(turns_purchase_sim/playing_days,2), daily_turns_wager_sim=round(turns_wager_sim/playing_days,2)) , .(userid, wheel_id, gamelevelid, isActiveDepositor, trstierid)], overwrite=TRUE) 

## We need to remove rows that landed on empty tile but passed milestone during that move - otherwise they would have counted as emty tile expirience (instead of milestone expirience)
dbWriteTable(con, "arena_board_experience_precomputed", ArenaPreCompGames[ !(sub_turn>1 & item_type==0), .(count = round(.N / uniqueN(iter),2), board_reward_tiles_multiplier = round(sum(board_reward_tiles_multiplier) / uniqueN(iter),2)) , .(wheel_id, chapter_id, board_id, item_type)], overwrite=TRUE) 
dbWriteTable(con, "arena_board_experience_simulated", ArenaSimRaw[ !(sub_turn>1 & item_type==0), .(count = round(.N / uniqueN(userid),2), tile_reward_sim = round(sum(tile_reward_sim) / uniqueN(userid),2)) , .(wheel_id, chapter_id, board_id, item_type)], overwrite=TRUE) 






## CHURN users should get this  CONFIG: WHEEL=6, 'turns_trans_effort' by 50$, 'effort_for_meter' based on MAX{10*Balance, MedianWagerNonDepByLevelGroup}
## NEW users should get DEFAULT CONFIG: WHEEL=6, 'turns_trans_effort' by 50$, 'effort_for_meter' based on NEW USRES Historical Wager
## Non Depos - what Wheel EV te set? Set configuration only for active users during SIM promo - instead of adding all 40M users to ArenaPers. Only in the End provide ArenaPers for all users.

