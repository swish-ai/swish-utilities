# HOW To Build dc-utilities

## Build and release using Github Actions CI
- [.github/workflows/build-release.yaml](.github/workflows/build-release.yaml) is [Github Action](https://docs.github.com/en/actions) to build and release for Windows, Linux and MacOS
- we use [GitVersion](https://gitversion.net/docs/) for automatic versioning with [mainline mode](https://gitversion.net/docs/reference/modes/mainline)

### Viewing Build Status and Logs
See [multi-utility Github Actions](https://github.com/swish-ai/multi-utility/actions)

### Automatic Build Triggers
by default only "master" branch push triggers Release Action  

### To release feature or dev branch add "#build" to git comment message. 
For example, you are working on "new-feature" branch
```
git branch --show-current
new-feature
```

```
git commit -am "added supercode and I want #build"
git push
```
It will create "v<versoion>-<branch-name>" release

### Manually Promoting major|minor|patch versions
add "+semver major|minor|patch" to the commit message:
```
git commit -am "#build +semver: minor -- it is a big feature"
```
```
git commit -am "#build +semver: major -- it is a big release"
```
see [Mainline gitversion](https://gitversion.net/docs/reference/modes/mainline)


## building executable manually
- we use [pyinstaller](https://pyinstaller.readthedocs.io/en/stable/)
```
pip3 install -r requirements.txt
pyinstaller --clean --onefile \
    --exclude-module matplotlib --exclude-module tkinter --exclude-module qt5 \
    --exclude-module python-dateutil --exclude-module pyinstaller --exclude-module tests \
    run.py -n dc-utilities
```

## references
- [pyinstaller](https://pyinstaller.readthedocs.io/en/stable/)
- [Github Action](https://docs.github.com/en/actions)
- [Virtual Environments](https://github.com/actions/virtual-environments)
- [GitVersion](https://gitversion.net/docs/) for automatic versioning with [mainline mode](https://gitversion.net/docs/reference/modes/mainline)
- [Release Action](https://github.com/softprops/action-gh-release)
- [Artifacts Action](https://docs.github.com/en/actions/advanced-guides/storing-workflow-data-as-artifacts)
- [Slack Notify Webhook](https://github.com/marketplace/actions/slack-notify)

