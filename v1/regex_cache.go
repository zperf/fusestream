package fusestream

import (
	"regexp"
	"sync"
)

type RegexCache struct {
	mutex sync.RWMutex
	re    map[string]*regexp.Regexp
}

func NewRegexCache() *RegexCache {
	return &RegexCache{
		re: make(map[string]*regexp.Regexp),
	}
}

func (f *RegexCache) Clear() {
	f.mutex.Lock()
	f.re = make(map[string]*regexp.Regexp)
	f.mutex.Unlock()
}

func (f *RegexCache) Compile(r string) (*regexp.Regexp, error) {
	f.mutex.RLock()
	re, ok := f.re[r]
	f.mutex.RUnlock()

	if ok {
		return re, nil
	}

	regex, err := regexp.Compile(r)
	if err != nil {
		return nil, err
	}

	f.mutex.Lock()
	f.re[r] = regex
	f.mutex.Unlock()

	return regex, nil
}
