FROM ruby:2.2.0

RUN apt-get update -qq && apt-get install -y build-essential

RUN mkdir -p /ambroi-people-search

WORKDIR /ambroi-people-search

ADD Gemfile /ambroi-people-search/Gemfile
ADD Gemfile.lock /ambroi-people-search/Gemfile.lock
RUN bundle install

ADD . /ambroi-people-search

