package multigeneration

type lruElement struct {
	newer *lruElement
	older *lruElement
	key   string
}

type lruCache struct {
	// Doubly linked list for storing elements in eviction order
	newest *lruElement
	oldest *lruElement

	// lookup table for quick check of the presence of the element
	elements map[string]*lruElement

	count   uint32
	maxSize uint32
}

func NewLRUCache(maxSize uint32) *lruCache {
	return &lruCache{
		newest:   nil,
		oldest:   nil,
		elements: map[string]*lruElement{},
		count:    uint32(0),
		maxSize:  maxSize,
	}
}

func (c *lruCache) Has(x string) bool {
	e, present := c.elements[x]
	if present {
		c.touch(e)
	}
	return present
}

func (c *lruCache) Add(x string) {
	if e, present := c.elements[x]; present {
		c.touch(e)
		return
	}
	e := &lruElement{
		key: x,
	}
	c.addToFront(e)
	c.elements[x] = e
	c.count++
}

func (c *lruCache) addToFront(e *lruElement) {

	e.older = c.newest
	e.newer = nil
	if c.newest != nil {
		c.newest.newer = e
	}

	c.newest = e
	if c.count >= c.maxSize {
		c.Prune()
	}

	if c.oldest == nil {
		c.oldest = e
	}
}

func (c *lruCache) removeFromList(e *lruElement) {
	// assumption: e does belong to list

	if e.older != nil {
		e.older.newer = e.newer
	} else { // is the oldest node
		c.oldest = e.newer
	}

	if e.newer != nil {
		e.newer.older = e.older
	} else { // is the newest node
		c.newest = e.older
	}

	e.older = nil
	e.newer = nil
}

func (c *lruCache) touch(e *lruElement) {
	c.removeFromList(e)
	c.addToFront(e)
}

func (c *lruCache) Prune() {
	if c.oldest == nil {
		return
	}
	key := c.oldest.key
	c.removeFromList(c.oldest)
	delete(c.elements, key)
	c.count--
}
