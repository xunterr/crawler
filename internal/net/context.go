package net

type Context struct {
	node     string
	scope    string
	metadata map[string][]byte
}

func NewContext(node string, req *Request, metadata map[string][]byte) *Context {
	return &Context{
		node:     node,
		scope:    req.Scope,
		metadata: metadata,
	}
}

func (rc *Context) Addr() string {
	return rc.node
}

func (rc *Context) Scope() string {
	return rc.scope
}

func (rc *Context) Metadata() map[string][]byte {
	return rc.metadata
}
