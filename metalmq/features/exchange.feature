Feature: Exchange

    Scenario: Declaring an exchange
        Given a client
        When declare an exchange test
        Then it succeeds

    Scenario: Channel close on passive declare a non-existent exchange
        Given a client
        When passive declare an exchange non-existent
        Then it closes the channel with error code 404

    Scenario: Check if exchange is created
        Given a client
        When an exchange is declared with name new-exchange
        Then passively declare exchange new-exchange succeeds
