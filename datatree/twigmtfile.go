package datatree



func NewHPFile(blockSize int, dirName string) (*HPFile, error) {
func (hpf *HPFile) Size() int64 {
func (hpf *HPFile) Truncate(size int64) error {
func (hpf *HPFile) Sync() error {
func (hpf *HPFile) Close() error {
func (hpf *HPFile) ReadAt(b []byte, off int64) error {
func (hpf *HPFile) Append(b []byte) error {
func (hpf *HPFile) PruneHead(off int64) error {
