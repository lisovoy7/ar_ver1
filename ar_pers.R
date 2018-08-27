library(RODBC)
library(data.table)
library(dplyr)
library(sqldf)
library(matrixStats)
library(RcppRoll)
library(DBI)
library(odbc)
library(readr)

Sys.time()

setwd("/home/andrey.lisovoy/TEMP_ANDREY/Arena/")

connImpala <- "Impala"
connAnalysis <- paste("Driver=ODBC Driver 13 for SQL Server; Server=10.145.16.22; Database=Analysis;    Uid=r_reader; Pwd=hIDGPJ4ioW2R5AaW7ap7" ,sep='' )
connHBIPA    <- paste("Driver=ODBC Driver 13 for SQL Server; Server=10.145.16.21; Database=Dwh_Pacific; Uid=r_reader; Pwd=hIDGPJ4ioW2R5AaW7ap7" ,sep='' )

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
  while(i < ceiling(nrow(df)/1024) ){
    dbWriteTable(channel.name, impala.table, df[ (1024*i+1) : min(1024*(i+1) , nrow(df)) ], append=T)
    i <- i+1
  }
  dbDisconnect(channel.name)
}Pro

hbipa.query = function (channel.name , query){
  channel <- odbcDriverConnect(channel.name)
  dt <- data.table(sqlQuery(channel, query, stringsAsFactors = FALSE))
  odbcClose(channel)
  dt
}

find_best_wheel = function(turns){
  min.wager.turns.promo <- promo.duration * turns_per_meter * min.wager.bar.fill.daily
  as.numeric(TurnsToCompletePerWheel[pmin(TurnsToCompletePerWheel$turns_to_complete - min.wager.turns.promo, 0.85 * TurnsToCompletePerWheel$turns_to_complete) > turns, max(wheel_id, min(TurnsToCompletePerWheel$wheel_id))])
}

ArenaWheelConfiguration <- as.data.table(read_delim("~/TEMP_ANDREY/Arena/arena_wheel_configuration.csv", "\t", escape_double = FALSE, trim_ws = TRUE))
ArenaWeelEV <- ArenaWheelConfiguration[ , .(ev = sum(steps * probability)) , wheel_id]

ArenaPreCompGames <- readRDS(file = "ArenaPreCompGames.rds")
ArenaPreCompGames[ , item_type_global := floor(item_type)]
TurnsToCompletePerWheel <- ArenaPreCompGames[item_type==4 & board_id==3 & chapter_id==3, .(turns_to_complete = ceiling(mean(turns_spent))), .(wheel_id)]

TilesRewardFromSim <- ArenaPreCompGames[, .(total_board_reward_tiles_multiplier = sum(board_reward_tiles_multiplier, na.rm=T)/uniqueN(iter)), .(wheel_id, chapter_id, board_id)]
names(TilesRewardFromSim)[names(TilesRewardFromSim) == "wheel_id"] <- "wheel.id"

ArenaRewardsDistConf <- as.data.table(read_delim("~/TEMP_ANDREY/Arena/arena_rewards_dist_conf.csv", "\t", escape_double = FALSE, trim_ws = TRUE))
ArenaRewardsDistConf[ , reward.prcnt := weight / sum(weight)]

max_cycles <- 10
min.wager.bar.fill.daily <- 1
turns.tutorial <- 2
turns_per_meter <- 3
play_again_grand_reward_mult <- 1.2
arena_reward_on_purchase <- 0.8
promo.duration <- 14
hist.duration <- 180 ##365
buffer.hist.to.sim <- 0

depo.min.arena.all.rewards <- 200000 
non.depo.max.arena.all.rewards <- 500e6 ## based on Legends Grand Reward for non depositors
uplift.depo <- (0.1)
uplift.non.depo <- 0.9
non.depo.reward.payout <- 0.050
new.users.reward.payout <- 0.055
wager.effort.depo.base.coins <-     18000 * 10 * 10 ## mult=dynamic ; 5$=18,000 ; 50$/5$=10 ; 10=wager.coef 
wager.effort.non.depo        <- 3 * 18000 * 4  * 10 ## mult=3       ; 5$=18,000 ; 20$/5$=4  ; 10=wager.coef 
wager.effort.new.users       <- 3 * 18000 * 4  * 10 ## mult=3       ; 5$=18,000 ; 20$/5$=4  ; 10=wager.coef 

shift_days <- 100
decay_ratio <- 0.07
floor_perc <- 0.5

sim.date.start <- as.Date('2018-07-26')
sim.dateid.start = as.integer(gsub("-", "", sim.date.start ))
sim.dateid.end = as.integer(gsub("-", "", (sim.date.start+(promo.duration-1)) ))
sim.syssnapshotdateid.end = as.integer(gsub("-", "", (sim.date.start+(promo.duration-1)+1) ))
seg.dateid.start = as.integer(gsub("-", "", (sim.date.start-(hist.duration+buffer.hist.to.sim) )))
seg.dateid.end = as.integer(gsub("-", "", (sim.date.start-(buffer.hist.to.sim+1) )))
seg.date.end = sim.date.start-(buffer.hist.to.sim+1) 
sim.month.start = as.numeric(substr(as.character(sim.dateid.start),1,6))
sim.month.end = as.numeric(substr(as.character(sim.dateid.end),1,6))
seg.syssnapshotdateid.end = as.integer(gsub("-", "", (sim.date.start-(buffer.hist.to.sim))))
seg.month.start <- as.numeric(substr(as.character(seg.dateid.start),1,6))
seg.month.end <- as.numeric(substr(as.character(seg.dateid.end),1,6))

PriceListTable <- as.data.table(read_delim("~/TEMP_ANDREY/Arena/arena_price_list.csv", "\t", escape_double = FALSE, trim_ws = TRUE))
impala.save("andrey", "arena_price_list", PriceListTable[,c("denom", "coins", "turns")])



DepoPers <- impala.query(connImpala , paste("
                            with
                            trns as(
                              select userid, date_ts, dateid, gross_amount, turns, moffercoinsamount, ",(1-floor_perc), "*( 1/(1+exp(",decay_ratio,"*(datediff(",paste0("'",seg.date.end,"'"),", date_ts)-",shift_days,"))) )+",floor_perc,"  as decay
                              from(
                                select userid, to_date(ts) as date_ts, dateid, sum(mamount) as gross_amount, sum(turns) as turns, sum(b.coins) as moffercoinsamount
                                from dwh.dwh_fact_transaction as a
                                join andrey.arena_price_list as b on round(a.mamount)=b.denom
                                where transtatusid=1 and triggerid !=18 and dateid between ",seg.dateid.start, " and ", seg.dateid.end, "
                                group by 1,2,3
                              ) as tmp
                            )
                            ,extreme_gross_amount as(
                              select userid, date_ts, decay, gross_amount as gross_amount_hist, gross_amount_decay, turns as turns_purchase_hist, turns_decay as turns_purchase_hist_decay, moffercoinsamount_hist_decay
                                    , case when tmp3.dateid > ",as.integer(gsub('-', '', (seg.date.end-promo.duration+1)))," then ",as.integer(gsub('-', '', (seg.date.end-promo.duration+1)))," else tmp3.dateid end as dateid_from
                                    , case when d.dateid > ",seg.dateid.end," then ",seg.dateid.end," else d.dateid end as dateid_to
                              from(
                                select userid, date_ts, dateid, decay, gross_amount, gross_amount_decay, turns, turns_decay, moffercoinsamount_hist_decay
                                from(
                                  select userid, date_ts, dateid, decay, gross_amount, decay*gross_amount as gross_amount_decay, turns, decay*turns as turns_decay, decay*moffercoinsamount as moffercoinsamount_hist_decay
                                          , row_number() over (partition by userid order by decay*turns desc) as rn
                                  from(
                                    select a.userid, a.date_ts, a.dateid, min(a.decay) as decay, sum(b.gross_amount) as gross_amount, sum(b.turns) as turns, sum(b.moffercoinsamount) as moffercoinsamount
                                    from trns a 
                                    JOIN trns b on a.userid=b.userid and b.date_ts between a.date_ts  and date_add(a.date_ts, (", promo.duration - 1, ")) 
                                    group by 1,2,3
                                  ) as tmp
                                ) as tmp2
                                where rn=1
                              ) as tmp3
                              join dwh.dwh_dim_date as d on date_add(tmp3.date_ts, (", promo.duration - 1, "))=d.dateshort 
                              where turns_decay >= 1
                            )
                            select b.*, nvl(wager_hist_decay,0) as wager_hist_decay, moffercoinsamount_hist_decay * nvl(l.offer_coins_multiplier_base, 1) as coins_purchase_hist, nvl(l.offer_coins_multiplier_base, 1) as offer_coins_multiplier_base, balance, nvl(threemonth10lpd_90pctbetamount,0) as threemonth10lpd_90pctbetamount
                            from(
                              select a.userid, sum(b.mtotalbetsamount*decay) as wager_hist_decay
                              from extreme_gross_amount as a
                              left join dwh.dwh_fact_spin_agg as b on a.userid=b.userid and b.dateid between dateid_from and dateid_to and month>=",seg.month.start," and mtotalbetsamount>0
                              group by 1
                            ) as tmp
                            join extreme_gross_amount as b on b.userid=tmp.userid
                            left join dwh.dwh_dim_user_hist as h on h.userid=b.userid and syssnapshotdateid=" ,seg.syssnapshotdateid.end, "
                            left join sources.mrr_level as l on l.level_id = h.gamelevelid and l.platform_type_id=1 and l.group_id=1
                            order by 1"))

## turns effort from purchases should be capped (relevant for users who got too many turns during hist period):
DepoPers[ , turns_trans_effort := pmin((1+uplift.depo) * turns_purchase_hist_decay, (max(TurnsToCompletePerWheel$turns_to_complete) - (promo.duration * turns_per_meter * min.wager.bar.fill.daily)))]

DepoPers[ , wheel_id := sapply(turns_trans_effort, find_best_wheel)]
DepoPers <- merge(DepoPers, TurnsToCompletePerWheel, by.x= c('wheel_id'), by.y= c('wheel_id'), all.x = TRUE, all.y = FALSE)

DepoPers[ , turns_wager_effort := turns_to_complete - turns_trans_effort]
DepoPers[ , hist.progress := turns_trans_effort / turns_purchase_hist_decay] ## 'hist.progress': in case user got too many turns from purchase he will finish arena several times. so we need to find the 'point' (wager) where he just finished 1st cycle:
DepoPers[ , effort_for_meter := (pmax(10 * coins_purchase_hist, offer_coins_multiplier_base * wager.effort.depo.base.coins, pmin(balance * 10, promo.duration * ifelse(threemonth10lpd_90pctbetamount==0, NA, threemonth10lpd_90pctbetamount), na.rm = T), wager_hist_decay) * hist.progress) / (turns_wager_effort / turns_per_meter)]

DepoPers[ , arena_all_rewards := pmax(depo.min.arena.all.rewards, arena_reward_on_purchase * hist.progress * coins_purchase_hist)]
DepoPers[ , arena_initial_grand_reward := arena_all_rewards * ArenaRewardsDistConf[is.na(chapter_id) & is.na(board_id), reward.prcnt] ]
DepoPers[ , arena_grand_reward := play_again_grand_reward_mult * arena_initial_grand_reward]

for(i in apply(ArenaRewardsDistConf[!is.na(chapter_id) & !is.na(board_id) & is.tile.reward==1, ], 1, as.list)){
  reward.prcnt <- ArenaRewardsDistConf[chapter_id==i$chapter_id & board_id==i$board_id & is.tile.reward==1, reward.prcnt]
  for(j in unique(DepoPers$wheel_id)){
    total_board_reward_tiles_multiplier <- TilesRewardFromSim[wheel.id==j & chapter_id==i$chapter_id & board_id==i$board_id, total_board_reward_tiles_multiplier]
    DepoPers[wheel_id == j , paste0("chapter",i$chapter_id,"_board",i$board_id,"_tiles_reward") := (reward.prcnt * arena_all_rewards) /  total_board_reward_tiles_multiplier] 
  }
}

saveRDS(DepoPers, file = "DepoPers.rds")

############################################################## Non Depo + Non Paying Depo + Churn Depo ############################################################
RestOfUsers <- impala.query(connImpala , paste("
select a.userid, extreme_wager_14d, nvl(threemonth10lpd_90pctbetamount,0) as threemonth10lpd_90pctbetamount, balance
from dwh.dwh_dim_user_hist as a
left join dwh.dwh_dim_user_dl as b on a.userid=b.userid
where gamelevelid >= 25 and syssnapshotdateid=" ,seg.syssnapshotdateid.end))
RestOfUsers <- RestOfUsers[ !userid %in% DepoPers$userid, ]

wheel.id.non.dep <- ArenaWeelEV[which.min(ArenaWeelEV$ev), wheel_id] ## Wheel with MIN EV - we want them to make many turns - it will allow them to fill the wager bar at least once a day
RestOfUsers[ , ':=' (  wheel_id = wheel.id.non.dep, turns_to_complete = TurnsToCompletePerWheel[wheel_id==wheel.id.non.dep , turns_to_complete])]
## MAX between 'extreme_wager' and 'balance-based wager' capped by 'real-wager' user did in the past. + CAP of MIN Effort (Min Reward should be at least appealing...) 
RestOfUsers[ , wager_hist_decay := pmax(pmin(balance * 10, promo.duration * ifelse(threemonth10lpd_90pctbetamount==0, NA, threemonth10lpd_90pctbetamount), na.rm = T), (1 + uplift.non.depo) * extreme_wager_14d, wager.effort.non.depo, na.rm = T)]
RestOfUsers[ , ':=' (effort_for_meter = wager_hist_decay / (turns_to_complete / turns_per_meter), arena_all_rewards = pmin(non.depo.max.arena.all.rewards, ceiling(non.depo.reward.payout * wager_hist_decay)))]
RestOfUsers[ , arena_initial_grand_reward := arena_all_rewards * ArenaRewardsDistConf[is.na(chapter_id) & is.na(board_id), reward.prcnt] ]
RestOfUsers[ , arena_grand_reward := play_again_grand_reward_mult * arena_initial_grand_reward]

for(i in apply(ArenaRewardsDistConf[!is.na(chapter_id) & !is.na(board_id) & is.tile.reward==1, ], 1, as.list)){
  total_board_reward_tiles_multiplier <- TilesRewardFromSim[wheel.id==wheel.id.non.dep & chapter_id==i$chapter_id & board_id==i$board_id, total_board_reward_tiles_multiplier]
  RestOfUsers[ , paste0("chapter",i$chapter_id,"_board",i$board_id,"_tiles_reward") := (arena_all_rewards * ArenaRewardsDistConf[chapter_id==i$chapter_id & board_id==i$board_id & is.tile.reward==1, reward.prcnt]) / total_board_reward_tiles_multiplier] }

ArenaPers <- rbind(DepoPers, RestOfUsers[, -c("extreme_wager_14d", "threemonth10lpd_90pctbetamount", "balance")], use.names = TRUE, fill = TRUE)
ArenaPers[ , persid := userid] ## for non-new users (depos + non-depos) the KEY is 'userid'
####################################################################################################################################################################


############################################################## DEFAULT Configuration for New Users #################################################################
wheel.id.new.users <- ArenaWeelEV[which.min(ArenaWeelEV$ev), wheel_id] ## Wheel with MIN EV - we want them to make many turns - it will allow them to fill the wager bar at least once a day

DefaultPers <- data.table(userid = 0, wheel_id = wheel.id.new.users, turns_to_complete = TurnsToCompletePerWheel[wheel_id==wheel.id.non.dep , turns_to_complete], arena_all_rewards = ceiling(new.users.reward.payout * wager.effort.new.users))
DefaultPers[ , effort_for_meter := ceiling(wager.effort.new.users / (turns_to_complete / turns_per_meter))]
DefaultPers[ , arena_initial_grand_reward := arena_all_rewards * ArenaRewardsDistConf[is.na(chapter_id) & is.na(board_id), reward.prcnt]]
DefaultPers[ , arena_grand_reward := play_again_grand_reward_mult * arena_initial_grand_reward]
DefaultPers[ , persid := 0]

for(i in apply(ArenaRewardsDistConf[!is.na(chapter_id) & !is.na(board_id) & is.tile.reward==1, ], 1, as.list)){
  total_board_reward_tiles_multiplier <- TilesRewardFromSim[wheel.id == wheel.id.new.users & chapter_id==i$chapter_id & board_id==i$board_id, total_board_reward_tiles_multiplier]
  DefaultPers[ , paste0("chapter",i$chapter_id,"_board",i$board_id,"_tiles_reward") := (arena_all_rewards * ArenaRewardsDistConf[chapter_id==i$chapter_id & board_id==i$board_id & is.tile.reward==1, reward.prcnt]) / total_board_reward_tiles_multiplier] }

ArenaPers <- rbind(ArenaPers, DefaultPers, use.names = TRUE, fill = TRUE)
####################################################################################################################################################################


for(i in apply(ArenaRewardsDistConf[!is.na(chapter_id) & is.na(board_id), ], 1, as.list)){
  ArenaPers[ , paste0("chapter",i$chapter_id,"_grand_reward") := arena_all_rewards * i$reward.prcnt] }

for(i in apply(ArenaRewardsDistConf[!is.na(chapter_id) & !is.na(board_id) & is.tile.reward==0, ], 1, as.list)){
  ArenaPers[ , paste0("chapter",i$chapter_id,"_board",i$board_id,"_grand_reward") := arena_all_rewards * i$reward.prcnt] }

ArenaPers[ , SysLoadingDate := Sys.time()]
saveRDS(ArenaPers, file = "ArenaPers.rds")






###########################################################################################################################################################################################
###########################################################################################################################################################################################
##################################################################################### SIMULATION ##########################################################################################
ArenaSim <- impala.query(connImpala , paste("
                                           with
                                           trans as(
                                                 select userid, gross_amount, turns_purchase, moffercoinsamount
                                                 from(
                                                 select userid, sum(mamount) as gross_amount, sum(turns) as turns_purchase, sum(b.coins) as moffercoinsamount
                                                 from dwh.dwh_fact_transaction as a
                                                 join andrey.arena_price_list as b on round(a.mamount)=b.denom
                                                 where transtatusid=1 and triggerid != 18 and dateid between ",sim.dateid.start, " and ", sim.dateid.end, "
                                                 group by 1
                                                 ) as tmp
                                               )
                                          ,wager as(
                                                 select userid, sum(wager) as wager, sum(case when spins > 50 then 1 else 0 end) as playing_days
                                                 from(
                                                   select userid, dateid, sum(mtotalbetsamount) as wager, sum(mbetscount) as spins
                                                   from dwh.dwh_fact_spin_agg 
                                                   where month>=",seg.month.start," and dateid between ",sim.dateid.start, " and ", sim.dateid.end, " and mtotalbetsamount>0 
                                                   group by 1,2
                                                 ) as tmp
                                                 group by 1
                                               )
                                           select wager.userid, threemonth10lpd_90pctspins, nvl(moffercoinsamount,0) * nvl(level.offer_coins_multiplier_base, 1) as coins_purchase_sim, nvl(h.gamelevelid,1) as gamelevelid, threemonth10lpd_freqbet, nvl(l.cashierlevelgroup, 'Unknown') as cashierlevelgroup, h.trstierid, h.deposithabitgroupid, nvl(wager,0) as wager_sim, nvl(playing_days,0) as playing_days, nvl(gross_amount,0) as gross_amount_sim , nvl(turns_purchase,0) as turns_purchase_sim, nvl(moffercoinsamount,0) as moffercoinsamount_sim, ",turns.tutorial, " as turns_tutorial_sim
                                           from wager
                                           left join trans on wager.userid=trans.userid
                                           left join dwh.dwh_dim_user_hist as h on wager.userid=h.userid and syssnapshotdateid=" ,sim.syssnapshotdateid.end,"
                                           left join dwh.v_dwh_dim_gamelevel as l on l.gamelevelid = nvl(h.gamelevelid,1)
                                           left join sources.mrr_level as level on level.level_id = h.gamelevelid and level.platform_type_id=1 and level.group_id=1"))

ArenaSim[ , isActiveDepositor := ifelse(gross_amount_sim > 20 , 1, 0)]
ArenaSim[ , persid := userid]
ArenaSim[ !(userid %in% ArenaPers$userid) , persid := 0]
ArenaSim <- merge(ArenaPers[, -"userid"], ArenaSim, by = c('persid'), all.x = FALSE,all.y = TRUE) ## Right Join (in order to keep logical order of columns I put first HIST data)
ArenaSim[ , turns_wager_sim := ceiling(turns_per_meter * (wager_sim / effort_for_meter))]
ArenaSim[ , turns_total_sim := turns_purchase_sim + turns_wager_sim + turns_tutorial_sim]
ArenaSim[ , iter := sample((max(ArenaPreCompGames$iter) - max_cycles), nrow(ArenaSim), replace = T)]
ArenaSim <- merge(ArenaSim, ArenaPreCompGames[ , .(turns_spent_across_iter_from = min(turns_spent_across_iter)), .(wheel_id, iter)], by = c('wheel_id', 'iter'), all.x = TRUE,all.y = FALSE)
ArenaSim[playing_days>0, ':=' (daily_turns_purchase_sim = round(turns_purchase_sim/playing_days,2), daily_turns_wager_sim = round(turns_wager_sim/playing_days,2), daily_wager_meter_sim = round((turns_wager_sim / turns_per_meter) / playing_days,2))]
ArenaSim[ , isDepositor := ifelse(is.na(gross_amount_decay), 0, 1)]
ArenaSim[ , isNewUser := ifelse(persid == 0, 1, 0)]

summary(ArenaSim)

ArenaSimRaw <- as.data.table(sqldf(paste0("
SELECT userid, (b.iter-a.iter + 1) as cycle_id , chapter_id, board_id, item_type, sum(board_reward_tiles_multiplier) as board_reward_tiles_multiplier, sum(board_reward_prcnt) as board_reward_prcnt, sum(chapter_reward_prcnt) as chapter_reward_prcnt, sum(grand_reward_prcnt) as grand_reward_prcnt, count(*) as cnt 
FROM ArenaSim as a 
LEFT JOIN ArenaPreCompGames as b on a.wheel_id = b.wheel_id 
                                     and b.iter >= a.iter 
                                     and b.iter <  a.iter + ", max_cycles, "
                                     and b.turns_spent_across_iter >= a.turns_spent_across_iter_from 
                                     and b.turns_spent_across_iter <  a.turns_spent_across_iter_from + a.turns_total_sim
GROUP BY userid, cycle_id, chapter_id, board_id, item_type")))
saveRDS(ArenaSimRaw, file = "ArenaSimRaw.rds")

columns_to_keep <- c("userid", "isNewUser", "isDepositor", "wheel_id", "gamelevelid", "threemonth10lpd_freqbet", "cashierlevelgroup", "trstierid", "deposithabitgroupid", "arena_all_rewards", "playing_days", "isActiveDepositor", "gross_amount_decay", "wager_hist_decay", "turns_trans_effort",  "turns_purchase_sim", "turns_wager_effort", "turns_wager_sim", "turns_to_complete", "turns_total_sim", ArenaRewardsDistConf[!is.na(chapter_id) & !is.na(board_id) & is.tile.reward==1, paste0("chapter",chapter_id,"_board",board_id,"_tiles_reward")])
ArenaSimRaw <- merge(ArenaSim[ , columns_to_keep, with=FALSE], ArenaSimRaw, by = c('userid'), all.x = TRUE, all.y = FALSE)

ArenaSimRaw[ , item_type_global := floor(item_type)]
for(i in apply(ArenaRewardsDistConf[!is.na(chapter_id) & !is.na(board_id) & is.tile.reward==1, ], 1, as.list)){
  ArenaSimRaw[chapter_id == i$chapter_id & board_id == i$board_id, tile_reward_sim := get(paste0("chapter",i$chapter_id,"_board",i$board_id,"_tiles_reward")) * board_reward_tiles_multiplier]
}
ArenaSimRaw[ , board_reward_sim := board_reward_prcnt * arena_all_rewards]
ArenaSimRaw[ , chapter_reward_sim := chapter_reward_prcnt * arena_all_rewards]
ArenaSimRaw[ , grand_reward_sim := grand_reward_prcnt * arena_all_rewards]

ArenaSim <- merge(ArenaSim, ArenaSimRaw[ , .(boards_completed = sum(ifelse(item_type == 4, 1, 0))), userid], by="userid", all.x = TRUE, all.y = FALSE)
ArenaSim <- merge(ArenaSim, ArenaSimRaw[ , .(cycles_completed = sum(ifelse(item_type == 4 & chapter_id == max(ArenaRewardsDistConf$chapter_id, na.rm = T) & board_id == max(ArenaRewardsDistConf$board_id, na.rm = T), 1, 0))), userid], by="userid", all.x = TRUE, all.y = FALSE)
ArenaSim <- merge(ArenaSim, ArenaSimRaw[ , .(all_rewards_sim = sum(tile_reward_sim + board_reward_sim + chapter_reward_sim + grand_reward_sim)), .(userid)], by="userid", all.x = TRUE, all.y = FALSE)
ArenaSim[ , rewards_out_of_wager_sim := round(all_rewards_sim / wager_sim, 2)]


setkey(ArenaSimRaw, userid, cycle_id, chapter_id, board_id, item_type)
saveRDS(ArenaSimRaw, file = "ArenaSimRaw.rds")

dbWriteTable(con, "arena_turns_acquired", ArenaSim, overwrite=TRUE) 
## We need to remove rows that landed on empty tile but passed milestone during that move - otherwise they would have counted as emty tile expirience (instead of milestone expirience)
# dbWriteTable(con, "arena_precomputed_games", ArenaPreCompGames[ !(sub_turn>1 & item_type==0), .(count = .N, board_reward_tiles_multiplier = sum(board_reward_tiles_multiplier)) , .(wheel_id, chapter_id, board_id, item_type_global)], overwrite=TRUE) 
dbWriteTable(con, "arena_simulation", ArenaSimRaw[, .(count = sum(cnt), tile_reward_sim = ceiling(sum(tile_reward_sim)), dc_users = uniqueN(userid)), .(isNewUser, isDepositor, trstierid, deposithabitgroupid, cashierlevelgroup, wheel_id, cycle_id, chapter_id, board_id, item_type_global)], overwrite=TRUE) 


################################################################################# Validation #################################################################################
# ArenaSim[playing_days>0, .('20%' = quantile(daily_wager_meter_sim, 0.1, na.rm = T), '50%' = quantile(daily_wager_meter_sim, 0.5, na.rm = T), '80%' = quantile(daily_wager_meter_sim, 0.9, na.rm = T), .N), .(wheel_id, isDepositor, isNewUser, isActiveDepositor, trstierid)][order(wheel_id, isDepositor, isNewUser, isActiveDepositor, trstierid)]
ArenaSim[playing_days>0, .('20%' = quantile(daily_wager_meter_sim, 0.1, na.rm = T), '50%' = quantile(daily_wager_meter_sim, 0.5, na.rm = T), '80%' = quantile(daily_wager_meter_sim, 0.9, na.rm = T), .N), .(wheel_id, isActiveDepositor, trstierid)][order(wheel_id, isActiveDepositor, trstierid)]

ArenaSim[isDepositor == 1 & isActiveDepositor == 1, .('20%' = quantile(turns_wager_sim/(turns_purchase_sim + turns_wager_sim), 0.1), '50%' = quantile(turns_wager_sim/(turns_purchase_sim + turns_wager_sim), 0.5),'80%' = quantile(turns_wager_sim/(turns_purchase_sim + turns_wager_sim), 0.9), .N), .(isActiveDepositor, wheel_id)][order(isActiveDepositor, wheel_id)]

View(ArenaSim[playing_days>0 , .(complete_rate = sum(ifelse(turns_total_sim >= turns_to_complete, 1, 0)) / .N, completers = sum(ifelse(turns_total_sim >= turns_to_complete, 1, 0)), count = .N), .(isDepositor, isNewUser, isActiveDepositor)][order(isDepositor, isNewUser, isActiveDepositor)])
# View(ArenaSim[playing_days>0 , .(complete_rate = sum(ifelse(turns_total_sim >= turns_to_complete, 1, 0)) / .N, completers = sum(ifelse(turns_total_sim >= turns_to_complete, 1, 0)), count = .N), .(isDepositor, isNewUser)][order(isDepositor, isNewUser)])

View(ArenaSim[playing_days>0 , .(complete_rate = sum(ifelse(cycles_completed>=1, 1, 0)) / .N, completers = sum(ifelse(cycles_completed>=1, 1, 0)), count = .N), .(isDepositor, isNewUser, isActiveDepositor)][order(isDepositor, isNewUser, isActiveDepositor)])
View(ArenaSim[playing_days>0 , .N, .(isActiveDepositor, isDepositor, isNewUser, pmin(boards_completed,9))][order(isActiveDepositor, isDepositor, isNewUser, pmin)])

View(ArenaPreCompGames[ item_type != 4 & (!(sub_turn>1 & item_type==0)), .(count = .N, board_reward_tiles_multiplier = round(sum(board_reward_tiles_multiplier),2), dc_iter = uniqueN(iter)) , .(wheel_id, chapter_id, board_id, item_type_global)][order(wheel_id, chapter_id, board_id, item_type_global)])
View(ArenaSimRaw[ item_type != 4 , .(tile_reward_sim = round(sum(tile_reward_sim))), .(wheel_id, chapter_id, board_id, item_type_global)][order(wheel_id, chapter_id, board_id, item_type_global)])

## Make sure the dist. of rewards is correct
View(ArenaSimRaw[cycle_id == 1 & userid %in% ArenaSim[cycles_completed>=1, userid], .(arena_all_rewards = mean(arena_all_rewards), tile_reward_sim = sum(tile_reward_sim), board_reward_sim = sum(board_reward_sim), chapter_reward_sim = sum(chapter_reward_sim), grand_reward_sim = sum(grand_reward_sim)), .(userid, chapter_id, board_id)]) 

View(ArenaSim[cycles_completed > 0 & isNewUser == 0 & isDepositor == 1 & isActiveDepositor == 1 & gross_amount_sim>40 & userid %in% ArenaSim[cycles_completed>=1, userid], .('20%' = quantile(all_rewards_sim/(coins_purchase_sim), 0.2, na.rm = T), '50%' = quantile(all_rewards_sim/(coins_purchase_sim), 0.5, na.rm = T),'80%' = quantile(all_rewards_sim/(coins_purchase_sim), 0.8, na.rm = T), .N), .(wheel_id, trstierid)][order(wheel_id, trstierid)]) 

## samples of single tile rewards....
(ArenaSimRaw[isNewUser==1 & cycle_id==1 & item_type %in% c(1,2), .(avg_reward = ceiling(sum(tile_reward_sim)/sum(cnt))), .(wheel_id, chapter_id, board_id, item_type_global)])


ArenaBoardConfiguration <- as.data.table(read_delim("~/TEMP_ANDREY/Arena/arena_board_configuration.csv", "\t", escape_double = FALSE, trim_ws = TRUE))
ArenaBoardConfiguration[ , reward_tiles_multiplier := reward_tiles_weight / sum(reward_tiles_weight, na.rm = T) , .(board_set_id, chapter_id, board_id)]

examplesOfUSers <- ArenaSim
examplesOfUSers[ , ':=' (typical_bonus_tile = ArenaBoardConfiguration[item_type_id==1 & chapter_id == 3 & board_id == 3, mean(reward_tiles_multiplier, na.rm = T)] * chapter3_board3_tiles_reward
                       , typical_milestone_tile = ArenaBoardConfiguration[item_type_id==2 & chapter_id == 3 & board_id == 3, mean(reward_tiles_multiplier, na.rm = T)] * chapter3_board3_tiles_reward
                       , effort_for_meter_freq_bet = ceiling(effort_for_meter / threemonth10lpd_freqbet), turns_wager_effort = ifelse(is.na(turns_wager_effort), turns_to_complete, turns_wager_effort), turns_trans_effort = ifelse(is.na(turns_trans_effort), 0, turns_trans_effort))]

examplesOfUSers[ , ':=' (typical_bonus_tile_freq_bet = typical_bonus_tile / threemonth10lpd_freqbet, typical_milestone_tile_freq_bet = typical_milestone_tile / threemonth10lpd_freqbet)]

examplesOfUSers <- examplesOfUSers[playing_days > 0, .(userid, isDepositor, isNewUser, isActiveDepositor, trstierid, deposithabitgroupid, cashierlevelgroup, threemonth10lpd_freqbet, threemonth10lpd_90pctspins, wheel_id, gross_amount_decay, gross_amount_sim, coins_purchase_hist, coins_purchase_sim, wager_hist_decay, wager_sim, turns_purchase_hist_decay, turns_purchase_sim, turns_wager_effort, turns_wager_sim, turns_to_complete, turns_trans_effort, turns_wager_effort, effort_for_meter, effort_for_meter_freq_bet
                                       , arena_all_rewards, arena_initial_grand_reward, daily_wager_meter_sim, boards_completed, cycles_completed, arena_all_rewards, all_rewards_sim, rewards_out_of_wager_sim, typical_bonus_tile, typical_milestone_tile, typical_bonus_tile_freq_bet, typical_milestone_tile_freq_bet
                                       , chapter1_grand_reward, chapter2_grand_reward, chapter3_grand_reward, chapter1_board1_grand_reward, chapter1_board2_grand_reward, chapter1_board3_grand_reward, chapter2_board1_grand_reward, chapter2_board2_grand_reward, chapter2_board3_grand_reward, chapter3_board1_grand_reward, chapter3_board2_grand_reward, chapter3_board3_grand_reward)]

write.csv(examplesOfUSers[sample(1:nrow(examplesOfUSers), ceiling(0.05*nrow(examplesOfUSers)), replace = F)], "examplesOfUSers.csv", row.names = F)

