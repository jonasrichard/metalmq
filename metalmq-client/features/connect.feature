Feature: Connect as guest

    Scenario: Connecting with plain password
        Given a user
        When connects as guest/guest
        Then it has been connected
