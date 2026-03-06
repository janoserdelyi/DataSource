
alias ll='ls -l --color=auto --group-directories-first' 
alias la='ls -la --color=auto --group-directories-first' 

#npm stuff
alias npm-global-installed='npm list -g --depth=0'

# a bunch of handy aliases from Pratyush
alias ga='git add .'
alias gs='git status'
alias gc='git commit -m'
alias gpo='git push origin'
alias gplo='git pull origin'
alias gco='git checkout'
alias gcob='git checkout -b'
alias gcom='git checkout main'
alias gcor='git checkout release'
alias gb='git branch'
alias gbd='git branch -d'
alias gd='git diff'
alias gplom='git pull origin main'
alias gcp='git cherry-pick'
alias grmif='git rm --cached'
alias grb='git recent-branches'
alias glm='git log --pretty=format:"%h%x09%an%x09%s"'
alias glm10='git log --pretty=format:"%h%x09%an%x09%s" | head -n 10'
alias glm20='git log --pretty=format:"%h%x09%an%x09%s" | head -n 20'

alias y='yarn'
alias yybd='yarn && yarn build-dev'
alias ybd='yarn build-dev'
alias yb='yarn build'
alias yyb='yarn && yarn build'
alias yfc='yarn flow check'
alias ys='yarn start'
alias ysd='yarn start-dev'
alias yw='yarn watch'
alias ywc='yarn watch:client'
alias yws='yarn watch:server'
alias ycw='yarn client-watch'
alias ysw='yarn server-watch'
alias ni='node --inspect-brk'

alias rmn='rimraf node_modules/'
alias rmd='rimraf dist/'
alias clr='clear'

alias lmc='yarn client-watch'
alias lms='yarn server-watch'

alias ni='node --inspect-brk'
alias yp='yarn pack'
alias ybyp='yb && yp'
alias ypb='yarn postbuild'

alias ysdb='yarn start:debug'

alias cl="git fetch -p && git for-each-ref --format '%(refname:short) %(upstream:track)' | awk '$2 == \"[gone]\" {print $1}' | xargs -r git branch -D"
alias db="git push --delete origin $1 && git branch -D "
alias cb="git checkout main && git pull origin main && git branch $1 && git checkout $1 && git push --set-upstream origin"


alias psql="/C/Program\ Files/PostgreSQL/17/bin/psql.exe"

mkcd () {
	mkdir -p -- "${1:?No target specified}" && cd -- "$1"
}

tf () {
	/c/Users/jerdelyi/utils/terraform_1.6.6_windows_amd64/terraform.exe "$@"
}

backfill () {
	~/projects/test/BackfillTrackingData/bin/Release/net10.0/BackfillTrackingData.exe "$@"
}

backfillalltypes () {
	for tt in sent open click unsub bounce; do 
		args=("$@")
		args+=("--tt")
		args+=("$tt")
		~/projects/test/BackfillTrackingData/bin/Release/net8.0/BackfillTrackingData.exe ${args[*]} 
	done
}

auditgenerator () {
	 ~/projects/test/AuditGenerator/bin/Release/net8.0/win-x64/publish/AuditGenerator.exe "$@"
}

sqlgenerator () {
	 ~/projects/test/SqlGenerator/bin/Release/net8.0/win-x64/publish/SqlGenerator.exe "$@"
}

injectorstats () {
	 ~/projects/test/FormInjectorStats/bin/Release/net9.0/win-x64/publish/FormInjectorStats.exe "$@"
}

usersinroles () {
	 ~/projects/test/UsersInRoles/bin/Debug/net9.0/UsersInroles.exe users "$@"
}

rolesforusers () {
	 ~/projects/test/UsersInRoles/bin/Debug/net9.0/UsersInroles.exe roles "$@"
}

rolesforcontacts () {
	 ~/projects/test/UsersInRoles/bin/Debug/net9.0/UsersInroles.exe contactids "$@"
}

authaws () {
	env="$1"
	rolename=""

	case $env in
		dev)
			rolename="aws_sharedsvc_dev_developer,aws_dataservices_dev_developer"
			;;
		tsm)
			rolename="aws_sharedsvc_tst_developer,aws_dataservices_tst_developer"
			;;
		prd)
			rolename="aws_sharedsvc_prd_developer,aws_dataservices_prd_developer"
			;;
	esac

	if [[ -z "$rolename" ]]; then
		echo "no rolename found. use environments dev, tsm, prd'"
		return -1
	fi

	auth2aws.exe login -r "$rolename"
}

if [ "$PWD" == '/' ]
then
        cd ~
fi
