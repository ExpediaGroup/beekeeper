package com.expediagroup.beekeeper.integration.utils;

import java.util.List;

import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;

public class EmptyPage<T> extends PageImpl<T> {

//    Here we override the page size so that we can construct an empty page when we want to test what happens
//    when the API doesn't find a table. The jackson dependency throws an error when we try to build a page of
//    size 0 and this class is a workaround to that issue. This class should only be used for that test and nothing else.

  @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
  protected EmptyPage(
      @JsonProperty("content") List<T> content,
      @JsonProperty("number") int number,
      @JsonProperty("size") int size,
      @JsonProperty("totalElements") Long totalElements,
      @JsonProperty("pageable") JsonNode pageable,
      @JsonProperty("last") boolean last,
      @JsonProperty("totalPages") int totalPages,
      @JsonProperty("sort") JsonNode sort,
      @JsonProperty("first") boolean first,
      @JsonProperty("numberOfElements") int numberOfElements,
      @JsonProperty("empty") boolean empty) {
    super(content, PageRequest.of(number, 1), totalElements);
  }

}
