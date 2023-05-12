/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

package model

import (
	"time"
)

// See: https://github.com/mailru/easyjson
type GithubRepoData struct {
	ID    int `json:"id"`
	Owner struct {
		Login             string `json:"login"`
		ID                int    `json:"id"`
		AvatarURL         string `json:"avatar_url"`
		GravatarID        string `json:"gravatar_id"`
		URL               string `json:"url"`
		HTMLURL           string `json:"html_url"`
		FollowersURL      string `json:"followers_url"`
		FollowingURL      string `json:"following_url"`
		GistsURL          string `json:"gists_url"`
		StarredURL        string `json:"starred_url"`
		SubscriptionsURL  string `json:"subscriptions_url"`
		OrganizationsURL  string `json:"organizations_url"`
		ReposURL          string `json:"repos_url"`
		EventsURL         string `json:"events_url"`
		ReceivedEventsURL string `json:"received_events_url"`
		Type              string `json:"type"`
		SiteAdmin         bool   `json:"site_admin"`
	} `json:"owner"`
	Name            string    `json:"name"`
	FullName        string    `json:"full_name"`
	Description     string    `json:"description"`
	Private         bool      `json:"private"`
	Fork            bool      `json:"fork"`
	URL             string    `json:"url"`
	HTMLURL         string    `json:"html_url"`
	CloneURL        string    `json:"clone_url"`
	GitURL          string    `json:"git_url"`
	SSHURL          string    `json:"ssh_url"`
	SvnURL          string    `json:"svn_url"`
	MirrorURL       string    `json:"mirror_url"`
	Homepage        string    `json:"homepage"`
	Language        any       `json:"language"`
	ForksCount      int       `json:"forks_count"`
	StargazersCount int       `json:"stargazers_count"`
	WatchersCount   int       `json:"watchers_count"`
	Size            int       `json:"size"`
	DefaultBranch   string    `json:"default_branch"`
	OpenIssuesCount int       `json:"open_issues_count"`
	HasIssues       bool      `json:"has_issues"`
	HasWiki         bool      `json:"has_wiki"`
	HasDownloads    bool      `json:"has_downloads"`
	PushedAt        time.Time `json:"pushed_at"`
	CreatedAt       time.Time `json:"created_at"`
	UpdatedAt       time.Time `json:"updated_at"`
	Permissions     struct {
		Admin bool `json:"admin"`
		Push  bool `json:"push"`
		Pull  bool `json:"pull"`
	} `json:"permissions"`
	SubscribersCount int `json:"subscribers_count"`
	Organization     struct {
		Login             string `json:"login"`
		ID                int    `json:"id"`
		AvatarURL         string `json:"avatar_url"`
		GravatarID        string `json:"gravatar_id"`
		URL               string `json:"url"`
		HTMLURL           string `json:"html_url"`
		FollowersURL      string `json:"followers_url"`
		FollowingURL      string `json:"following_url"`
		GistsURL          string `json:"gists_url"`
		StarredURL        string `json:"starred_url"`
		SubscriptionsURL  string `json:"subscriptions_url"`
		OrganizationsURL  string `json:"organizations_url"`
		ReposURL          string `json:"repos_url"`
		EventsURL         string `json:"events_url"`
		ReceivedEventsURL string `json:"received_events_url"`
		Type              string `json:"type"`
		SiteAdmin         bool   `json:"site_admin"`
	} `json:"organization"`
	Parent struct {
		ID    int `json:"id"`
		Owner struct {
			Login             string `json:"login"`
			ID                int    `json:"id"`
			AvatarURL         string `json:"avatar_url"`
			GravatarID        string `json:"gravatar_id"`
			URL               string `json:"url"`
			HTMLURL           string `json:"html_url"`
			FollowersURL      string `json:"followers_url"`
			FollowingURL      string `json:"following_url"`
			GistsURL          string `json:"gists_url"`
			StarredURL        string `json:"starred_url"`
			SubscriptionsURL  string `json:"subscriptions_url"`
			OrganizationsURL  string `json:"organizations_url"`
			ReposURL          string `json:"repos_url"`
			EventsURL         string `json:"events_url"`
			ReceivedEventsURL string `json:"received_events_url"`
			Type              string `json:"type"`
			SiteAdmin         bool   `json:"site_admin"`
		} `json:"owner"`
		Name            string    `json:"name"`
		FullName        string    `json:"full_name"`
		Description     string    `json:"description"`
		Private         bool      `json:"private"`
		Fork            bool      `json:"fork"`
		URL             string    `json:"url"`
		HTMLURL         string    `json:"html_url"`
		CloneURL        string    `json:"clone_url"`
		GitURL          string    `json:"git_url"`
		SSHURL          string    `json:"ssh_url"`
		SvnURL          string    `json:"svn_url"`
		MirrorURL       string    `json:"mirror_url"`
		Homepage        string    `json:"homepage"`
		Language        any       `json:"language"`
		ForksCount      int       `json:"forks_count"`
		StargazersCount int       `json:"stargazers_count"`
		WatchersCount   int       `json:"watchers_count"`
		Size            int       `json:"size"`
		DefaultBranch   string    `json:"default_branch"`
		OpenIssuesCount int       `json:"open_issues_count"`
		HasIssues       bool      `json:"has_issues"`
		HasWiki         bool      `json:"has_wiki"`
		HasDownloads    bool      `json:"has_downloads"`
		PushedAt        time.Time `json:"pushed_at"`
		CreatedAt       time.Time `json:"created_at"`
		UpdatedAt       time.Time `json:"updated_at"`
		Permissions     struct {
			Admin bool `json:"admin"`
			Push  bool `json:"push"`
			Pull  bool `json:"pull"`
		} `json:"permissions"`
	} `json:"parent"`
	Source struct {
		ID    int `json:"id"`
		Owner struct {
			Login             string `json:"login"`
			ID                int    `json:"id"`
			AvatarURL         string `json:"avatar_url"`
			GravatarID        string `json:"gravatar_id"`
			URL               string `json:"url"`
			HTMLURL           string `json:"html_url"`
			FollowersURL      string `json:"followers_url"`
			FollowingURL      string `json:"following_url"`
			GistsURL          string `json:"gists_url"`
			StarredURL        string `json:"starred_url"`
			SubscriptionsURL  string `json:"subscriptions_url"`
			OrganizationsURL  string `json:"organizations_url"`
			ReposURL          string `json:"repos_url"`
			EventsURL         string `json:"events_url"`
			ReceivedEventsURL string `json:"received_events_url"`
			Type              string `json:"type"`
			SiteAdmin         bool   `json:"site_admin"`
		} `json:"owner"`
		Name            string    `json:"name"`
		FullName        string    `json:"full_name"`
		Description     string    `json:"description"`
		Private         bool      `json:"private"`
		Fork            bool      `json:"fork"`
		URL             string    `json:"url"`
		HTMLURL         string    `json:"html_url"`
		CloneURL        string    `json:"clone_url"`
		GitURL          string    `json:"git_url"`
		SSHURL          string    `json:"ssh_url"`
		SvnURL          string    `json:"svn_url"`
		MirrorURL       string    `json:"mirror_url"`
		Homepage        string    `json:"homepage"`
		Language        any       `json:"language"`
		ForksCount      int       `json:"forks_count"`
		StargazersCount int       `json:"stargazers_count"`
		WatchersCount   int       `json:"watchers_count"`
		Size            int       `json:"size"`
		DefaultBranch   string    `json:"default_branch"`
		OpenIssuesCount int       `json:"open_issues_count"`
		HasIssues       bool      `json:"has_issues"`
		HasWiki         bool      `json:"has_wiki"`
		HasDownloads    bool      `json:"has_downloads"`
		PushedAt        time.Time `json:"pushed_at"`
		CreatedAt       time.Time `json:"created_at"`
		UpdatedAt       time.Time `json:"updated_at"`
		Permissions     struct {
			Admin bool `json:"admin"`
			Push  bool `json:"push"`
			Pull  bool `json:"pull"`
		} `json:"permissions"`
	} `json:"source"`
}
