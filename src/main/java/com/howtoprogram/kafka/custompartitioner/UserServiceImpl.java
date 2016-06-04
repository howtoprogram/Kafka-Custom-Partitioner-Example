package com.howtoprogram.kafka.custompartitioner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class UserServiceImpl implements IUserService {


  // Pairs of username and id
  private Map<String, Integer> usersMap;

  public UserServiceImpl() {
    usersMap = new HashMap<>();
    usersMap.put("Tom", 1);
    usersMap.put("Mary", 2);
    usersMap.put("Alice", 3);
    usersMap.put("Daisy", 4);
    usersMap.put("Helen", 5);
  }


  @Override
  public Integer findUserId(String userName) {
    return usersMap.get(userName);
  }


  @Override
  public List<String> findAllUsers() {
    return new ArrayList<>(usersMap.keySet());

  }

}
