# Databricks notebook source
input_file_RDD=sc.textFile("/FileStore/tables/soc_LiveJournal1Adj-2d179.txt")

def mutual_friend_mapper(line):
  user_friends=line.split("\t")
  users=user_friends[0]
  friends=user_friends[1].split(",");
  friends_second_list=list(friends)
  mutual_friend_list=[]
  if friends:
    for friend in friends:
      if(friend !=''):
        friends_second_list.remove(friend)
        friends_first_list=list(friends_second_list)
        if(int(friend)>int(users)):
          mutual_friend_list.append(((int(users),int(friend)),friends_first_list))
          friends_second_list.append(friend)
        else:
          mutual_friend_list.append(((int(friend),int(users)),friends_first_list))
          friends_second_list.append(friend)  
  return mutual_friend_list

friend_pair_RDD=input_file_RDD.flatMap(mutual_friend_mapper)
mutual_friends_RDD=friend_pair_RDD.reduceByKey(lambda list1,list2:list(set(list1).intersection(list2)))
mutual_friends_RDD.coalesce(1).saveAsTextFile('FileStore/q1_output')


# COMMAND ----------


